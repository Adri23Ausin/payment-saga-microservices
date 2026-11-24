package com.adrianausin.saga.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaEventSerde;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.inbox.ProcessedEvents;
import com.adrianausin.saga.common.outbox.Outbox;
import com.adrianausin.saga.common.testing.SagaContainers;
import com.adrianausin.saga.common.testing.SagaRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(classes = OutboxIT.TestApplication.class, properties = "spring.flyway.locations=classpath:db/saga")
class OutboxIT {

    @SpringBootApplication
    @Import(SagaContainers.class)
    static class TestApplication {
    }

    @Autowired
    private Outbox outbox;
    @Autowired
    private ProcessedEvents processedEvents;
    @Autowired
    private TransactionTemplate transaction;
    @Autowired
    private JdbcClient jdbc;

    @Test
    void eventsAddedInATransactionArePublishedWithKeyAndEventId() {
        UUID orderId = UUID.randomUUID();
        SagaEvent event = new SagaEvent.StockReserved(orderId);

        UUID eventId = transaction.execute(status -> outbox.add(SagaTopics.INVENTORY, event));

        try (KafkaConsumer<String, String> consumer = SagaRecords.consumer(SagaTopics.INVENTORY)) {
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                consumer.poll(Duration.ofMillis(300)).forEach(record -> {
                    if (record.key().equals(orderId.toString())) {
                        records.add(record);
                    }
                });
                return !records.isEmpty();
            });
            ConsumerRecord<String, String> record = records.getFirst();
            assertThat(SagaEventSerde.fromJson(record.value())).isEqualTo(event);
            assertThat(new String(record.headers().lastHeader(SagaTopics.EVENT_ID_HEADER).value(), StandardCharsets.UTF_8))
                    .isEqualTo(eventId.toString());
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> jdbc.sql("SELECT published_at IS NOT NULL FROM outbox WHERE id = :id")
                .param("id", eventId).query(Boolean.class).single());
    }

    @Test
    void eventsOfARolledBackTransactionAreNeverPublished() {
        UUID orderId = UUID.randomUUID();

        transaction.executeWithoutResult(status -> {
            outbox.add(SagaTopics.INVENTORY, new SagaEvent.StockReserved(orderId));
            status.setRollbackOnly();
        });

        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE message_key = :key")
                .param("key", orderId.toString()).query(Integer.class).single()).isZero();
    }

    @Test
    void eventsCannotBeAddedOutsideATransaction() {
        assertThatThrownBy(() -> outbox.add(SagaTopics.INVENTORY, new SagaEvent.StockReserved(UUID.randomUUID())))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void anEventIsProcessedOnlyOnce() {
        ConsumerRecord<String, String> delivery = record(UUID.randomUUID());

        assertThat(firstDelivery(delivery)).isTrue();
        assertThat(firstDelivery(delivery)).isFalse();
    }

    @Test
    void aFailedHandlingDoesNotMarkTheEventAsProcessed() {
        ConsumerRecord<String, String> delivery = record(UUID.randomUUID());

        transaction.executeWithoutResult(status -> {
            processedEvents.firstDelivery(delivery);
            status.setRollbackOnly();
        });

        assertThat(firstDelivery(delivery)).isTrue();
    }

    private boolean firstDelivery(ConsumerRecord<String, String> delivery) {
        return Boolean.TRUE.equals(transaction.execute(status -> processedEvents.firstDelivery(delivery)));
    }

    private static ConsumerRecord<String, String> record(UUID eventId) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(SagaTopics.ORDERS, 0, 0, 0L, TimestampType.CREATE_TIME,
                0, 0, "key", "{}", new RecordHeaders(), Optional.empty());
        record.headers().add(SagaTopics.EVENT_ID_HEADER, eventId.toString().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
