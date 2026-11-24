package com.adrianausin.saga.common.testing;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaEventSerde;
import com.adrianausin.saga.common.event.SagaTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Publishes and reads saga events in tests, as another service would. */
public final class SagaRecords {

    private SagaRecords() {
    }

    /** Publishes an event with the given id, like an outbox relay would. */
    public static void publish(String topic, SagaEvent event, UUID eventId) {
        publishRaw(topic, event.orderId().toString(), SagaEventSerde.toJson(event), eventId);
    }

    public static void publishRaw(String topic, String key, String value, UUID eventId) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SagaContainers.KAFKA.getBootstrapServers());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            record.headers().add(SagaTopics.EVENT_ID_HEADER, eventId.toString().getBytes(StandardCharsets.UTF_8));
            producer.send(record);
        }
    }

    /** A consumer that reads a topic from the beginning. */
    public static KafkaConsumer<String, String> consumer(String topic) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SagaContainers.KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    /** The events of one order seen on a topic so far. */
    public static List<SagaEvent> eventsOf(KafkaConsumer<String, String> consumer, UUID orderId, List<SagaEvent> seen) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
            if (record.key().equals(orderId.toString())) {
                seen.add(SagaEventSerde.fromJson(record.value()));
            }
        }
        return seen;
    }

    public static List<SagaEvent> newList() {
        return new ArrayList<>();
    }
}
