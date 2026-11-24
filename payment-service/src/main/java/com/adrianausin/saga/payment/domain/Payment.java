package com.adrianausin.saga.payment.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

public record Payment(UUID orderId, String customerId, BigDecimal amount, Status status, @Nullable String reason,
                      Instant createdAt, Instant updatedAt) {

    public enum Status {
        CHARGED, DECLINED, REFUNDED
    }
}
