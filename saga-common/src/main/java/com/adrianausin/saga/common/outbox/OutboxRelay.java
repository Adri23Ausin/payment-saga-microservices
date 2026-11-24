package com.adrianausin.saga.common.outbox;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.adrianausin.saga.common.event.SagaTopics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes outbox events to Kafka, oldest first, and marks them as published. Delivery is at
 * least once: if the service dies between sending and marking, the event is sent again, which is
 * why consumers are idempotent. {@code FOR UPDATE SKIP LOCKED} lets several instances relay
 * side by side without sending the same event twice.
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final Duration retention;

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate transaction, Clock clock,
                       Duration retention) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.transaction = transaction;
        this.clock = clock;
        this.retention = retention;
    }

    @Scheduled(fixedDelayString = "${saga.outbox.poll-interval:200ms}")
    public void relay() {
        Integer published;
        do {
            published = transaction.execute(status -> publishBatch());
        } while (published != null && published == BATCH_SIZE);
    }

    /** @return how many events were published */
    int publishBatch() {
        List<OutboxEvent> events = jdbc.sql("""
                        SELECT id, topic, message_key, payload FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY created_at, id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", BATCH_SIZE)
                .query((rs, row) -> new OutboxEvent(rs.getObject("id", UUID.class), rs.getString("topic"),
                        rs.getString("message_key"), rs.getString("payload")))
                .list();

        for (OutboxEvent event : events) {
            ProducerRecord<String, String> record = new ProducerRecord<>(event.topic(), event.key(), event.payload());
            record.headers().add(SagaTopics.EVENT_ID_HEADER, event.id().toString().getBytes(StandardCharsets.UTF_8));
            try {
                kafka.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                // Rolls back the batch; what was sent already will be sent again and ignored by consumers
                throw new IllegalStateException("Could not publish outbox event " + event.id(), e);
            }
            jdbc.sql("UPDATE outbox SET published_at = :now WHERE id = :id")
                    .param("now", Timestamp.from(clock.instant()))
                    .param("id", event.id())
                    .update();
        }
        if (!events.isEmpty()) {
            log.debug("Published {} outbox events", events.size());
        }
        return events.size();
    }

    @Scheduled(fixedDelayString = "${saga.outbox.cleanup-interval:1h}")
    public void deletePublished() {
        jdbc.sql("DELETE FROM outbox WHERE published_at < :before")
                .param("before", Timestamp.from(clock.instant().minus(retention)))
                .update();
    }

    private record OutboxEvent(UUID id, String topic, String key, String payload) {
    }
}
