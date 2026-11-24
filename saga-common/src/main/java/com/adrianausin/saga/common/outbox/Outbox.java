package com.adrianausin.saga.common.outbox;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaEventSerde;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Transactional outbox: an event is stored in the service's database, in the same transaction
 * as the change it describes, and {@link OutboxRelay} publishes it afterwards. Writing to the
 * database and to Kafka directly could leave one done and not the other.
 */
public class Outbox {

    private final JdbcClient jdbc;
    private final Clock clock;

    public Outbox(JdbcClient jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    /** @return the id of the event, also sent as a header so consumers can drop duplicates */
    public UUID add(String topic, SagaEvent event) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox events must be added inside the transaction that changes the data");
        }
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO outbox (id, topic, message_key, event_type, payload, created_at)
                        VALUES (:id, :topic, :key, :type, :payload, :createdAt)
                        """)
                .param("id", id)
                .param("topic", topic)
                .param("key", event.orderId().toString())
                .param("type", event.getClass().getSimpleName())
                .param("payload", SagaEventSerde.toJson(event))
                .param("createdAt", Timestamp.from(clock.instant()))
                .update();
        return id;
    }
}
