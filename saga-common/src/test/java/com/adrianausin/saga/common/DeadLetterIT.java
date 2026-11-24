package com.adrianausin.saga.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.testing.SagaContainers;
import com.adrianausin.saga.common.testing.SagaRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Events that can't be read, or keep failing, end up in the topic's .DLT and the partition moves on. */
@SpringBootTest(classes = DeadLetterIT.TestApplication.class, properties = {
        "spring.flyway.locations=classpath:db/saga",
        "spring.kafka.consumer.group-id=dead-letter-test",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer",
        "spring.kafka.consumer.properties.spring.deserializer.value.delegate.class="
                + "com.adrianausin.saga.common.event.SagaEventSerde$EventDeserializer"})
class DeadLetterIT {

    @SpringBootApplication
    @Import({SagaContainers.class, FailingListener.class})
    static class TestApplication {
    }

    /** Fails on orders whose id starts with 0000. */
    @Component
    static class FailingListener {

        @KafkaListener(topics = SagaTopics.ORDERS)
        void on(SagaEvent event) {
            if (event.orderId().toString().startsWith("00000000")) {
                throw new IllegalStateException("Handling failed");
            }
        }
    }

    @Test
    void unreadableAndFailingEventsAreParkedInTheDeadLetterTopic() {
        String unreadableKey = "unreadable-" + UUID.randomUUID();
        UUID failingOrder = UUID.fromString("00000000-0000-0000-0000-" + String.format("%012d", System.nanoTime() % 1_000_000_000_000L));

        SagaRecords.publishRaw(SagaTopics.ORDERS, unreadableKey, "this is not json", UUID.randomUUID());
        SagaRecords.publish(SagaTopics.ORDERS, new SagaEvent.StockReserved(failingOrder), UUID.randomUUID());

        try (KafkaConsumer<String, String> deadLetters = SagaRecords.consumer(SagaTopics.deadLetter(SagaTopics.ORDERS))) {
            List<ConsumerRecord<String, String>> parked = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                deadLetters.poll(Duration.ofMillis(500)).forEach(parked::add);
                return parked.stream().map(ConsumerRecord::key).toList().containsAll(List.of(unreadableKey, failingOrder.toString()));
            });
            assertThat(parked).filteredOn(record -> record.key().equals(unreadableKey))
                    .first().satisfies(record -> assertThat(record.value()).isEqualTo("this is not json"));
            assertThat(parked).filteredOn(record -> record.key().equals(failingOrder.toString()))
                    .first().satisfies(record -> assertThat(record.headers().lastHeader("kafka_dlt-exception-message")).isNotNull());
        }
    }
}
