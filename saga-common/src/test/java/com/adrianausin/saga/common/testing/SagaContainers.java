package com.adrianausin.saga.common.testing;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * PostgreSQL and Kafka for integration tests, shared through the test jar. Static, so every test
 * context in a module reuses the same containers.
 */
@TestConfiguration(proxyBeanMethods = false)
public class SagaContainers {

    public static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");
    public static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.0");

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return POSTGRES;
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return KAFKA;
    }
}
