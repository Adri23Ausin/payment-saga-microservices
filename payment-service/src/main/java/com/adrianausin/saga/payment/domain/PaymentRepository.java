package com.adrianausin.saga.payment.domain;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentRepository {

    private final JdbcClient jdbc;

    PaymentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<BigDecimal> balance(String customerId) {
        return jdbc.sql("SELECT balance FROM accounts WHERE customer_id = :customer")
                .param("customer", customerId)
                .query(BigDecimal.class)
                .optional();
    }

    /**
     * Takes the amount only if it is there, in one statement: two concurrent charges can't both
     * pass a check made before either of them updates.
     *
     * @return false if the balance was too low
     */
    public boolean withdraw(String customerId, BigDecimal amount) {
        return jdbc.sql("UPDATE accounts SET balance = balance - :amount WHERE customer_id = :customer AND balance >= :amount")
                .param("customer", customerId)
                .param("amount", amount)
                .update() == 1;
    }

    public void deposit(String customerId, BigDecimal amount) {
        jdbc.sql("UPDATE accounts SET balance = balance + :amount WHERE customer_id = :customer")
                .param("customer", customerId)
                .param("amount", amount)
                .update();
    }

    public void insert(UUID orderId, String customerId, BigDecimal amount, Payment.Status status, @Nullable String reason,
                       Instant now) {
        jdbc.sql("""
                        INSERT INTO payments (order_id, customer_id, amount, status, reason, created_at, updated_at)
                        VALUES (:order, :customer, :amount, :status, :reason, :now, :now)
                        """)
                .param("order", orderId)
                .param("customer", customerId)
                .param("amount", amount)
                .param("status", status.name())
                .param("reason", reason)
                .param("now", Timestamp.from(now))
                .update();
    }

    /** @return false unless the payment was charged; a refund happens at most once */
    public boolean markRefunded(UUID orderId, String reason, Instant now) {
        return jdbc.sql("""
                        UPDATE payments SET status = 'REFUNDED', reason = :reason, updated_at = :now
                        WHERE order_id = :order AND status = 'CHARGED'
                        """)
                .param("order", orderId)
                .param("reason", reason)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    public Optional<Payment> findByOrder(UUID orderId) {
        return jdbc.sql("SELECT * FROM payments WHERE order_id = :order")
                .param("order", orderId)
                .query((rs, row) -> new Payment(rs.getObject("order_id", UUID.class), rs.getString("customer_id"),
                        rs.getBigDecimal("amount"), Payment.Status.valueOf(rs.getString("status")), rs.getString("reason"),
                        rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant()))
                .optional();
    }
}
