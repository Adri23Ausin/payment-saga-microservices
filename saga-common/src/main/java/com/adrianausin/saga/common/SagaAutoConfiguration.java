package com.adrianausin.saga.common;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaEventSerde;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.inbox.ProcessedEvents;
import com.adrianausin.saga.common.outbox.Outbox;
import com.adrianausin.saga.common.outbox.OutboxRelay;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

/** Outbox, idempotent consumers, topics and dead-lettering for every service that has this module. */
@AutoConfiguration
@EnableScheduling
public class SagaAutoConfiguration {

    private static final int PARTITIONS = 3;
    private static final long RETRY_INTERVAL_MS = 1000;
    private static final long RETRIES = 3;

    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    Outbox outbox(JdbcClient jdbc, Clock clock) {
        return new Outbox(jdbc, clock);
    }

    @Bean
    OutboxRelay outboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate transaction,
                            Clock clock, @Value("${saga.outbox.retention:7d}") Duration retention) {
        return new OutboxRelay(jdbc, kafka, transaction, clock, retention);
    }

    @Bean
    ProcessedEvents processedEvents(JdbcClient jdbc, Clock clock) {
        return new ProcessedEvents(jdbc, clock);
    }

    /**
     * A failing event is retried a few times, then parked in {@code <topic>.DLT} so the partition
     * moves on. Unreadable events (the deserializer failed) go there straight away.
     */
    @Bean
    CommonErrorHandler sagaErrorHandler(KafkaProperties properties) {
        // Dead letters are either raw bytes (unreadable) or events (failed while being handled)
        var serializer = new DelegatingByTypeSerializer(
                Map.of(byte[].class, new ByteArraySerializer(), SagaEvent.class, new SagaEventSerde.EventSerializer()),
                true);
        var producerFactory = new DefaultKafkaProducerFactory<String, Object>(
                properties.buildProducerProperties(), new StringSerializer(), serializer);
        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(producerFactory));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, RETRIES));
    }

    @Bean
    KafkaAdmin.NewTopics sagaTopics() {
        return new KafkaAdmin.NewTopics(
                topic(SagaTopics.ORDERS), topic(SagaTopics.PAYMENTS), topic(SagaTopics.INVENTORY),
                deadLetter(SagaTopics.ORDERS), deadLetter(SagaTopics.PAYMENTS), deadLetter(SagaTopics.INVENTORY));
    }

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(PARTITIONS).build();
    }

    /** The recoverer keeps the partition of the failed record, so the dead-letter topic needs as many. */
    private static NewTopic deadLetter(String name) {
        return TopicBuilder.name(name + ".DLT").partitions(PARTITIONS).build();
    }
}
