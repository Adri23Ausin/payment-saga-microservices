package com.adrianausin.saga.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent.Item;
import org.jspecify.annotations.Nullable;

/** @param reason why the order was cancelled, null otherwise */
public record Order(UUID id, String customerId, Status status, List<Item> items, BigDecimal total,
                    @Nullable String reason, Instant createdAt, Instant updatedAt, List<HistoryEntry> history) {

    public enum Status {
        /** Placed; the saga is running. */
        PENDING,
        /** Paid and stock reserved. */
        CONFIRMED,
        /** Payment failed, or stock was short and the payment was refunded. */
        CANCELLED;

        public boolean isFinal() {
            return this != PENDING;
        }
    }

    public record HistoryEntry(Instant occurredAt, String step, @Nullable String detail) {
    }

    public static BigDecimal totalOf(List<Item> items) {
        return items.stream().map(Item::total).reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
