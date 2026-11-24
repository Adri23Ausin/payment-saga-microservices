package com.adrianausin.saga.common.inbox;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Idempotent consumers: events arrive at least once, so each one is recorded when handled, in the
 * same transaction as its effects. A redelivered event finds its record and is skipped; if the
 * handling fails, the record rolls back with everything else and the event is tried again.
 */
public class ProcessedEvents {

    private final JdbcClient jdbc;
    private final Clock clock;

    public ProcessedEvents(JdbcClient jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    /** @return true the first time the record's event is seen, false for a duplicate */
    public boolean firstDelivery(ConsumerRecord<String, ?> record) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Events must be marked as processed inside the transaction that handles them");
        }
        Header header = record.headers().lastHeader(SagaTopics.EVENT_ID_HEADER);
        if (header == null) {
            throw new IllegalArgumentException("Event without an " + SagaTopics.EVENT_ID_HEADER + " header");
        }
        UUID eventId = UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
        return jdbc.sql("""
                        INSERT INTO processed_events (event_id, topic, processed_at) VALUES (:id, :topic, :now)
                        ON CONFLICT (event_id) DO NOTHING
                        """)
                .param("id", eventId)
                .param("topic", record.topic())
                .param("now", Timestamp.from(clock.instant()))
                .update() == 1;
    }
}
