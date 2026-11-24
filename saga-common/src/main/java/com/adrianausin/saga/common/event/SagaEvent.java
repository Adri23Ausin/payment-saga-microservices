package com.adrianausin.saga.common.event;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Everything that happens in an order's saga. Each service publishes its own events and reacts
 * to the others'; nobody calls anybody. The JSON carries a {@code type} so a consumer knows which
 * record to read.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.SIMPLE_NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(SagaEvent.OrderCreated.class),
        @JsonSubTypes.Type(SagaEvent.PaymentCompleted.class),
        @JsonSubTypes.Type(SagaEvent.PaymentFailed.class),
        @JsonSubTypes.Type(SagaEvent.StockReserved.class),
        @JsonSubTypes.Type(SagaEvent.StockRejected.class),
        @JsonSubTypes.Type(SagaEvent.PaymentRefunded.class)
})
public sealed interface SagaEvent {

    UUID orderId();

    record Item(String sku, int quantity, BigDecimal unitPrice) {

        public BigDecimal total() {
            return unitPrice.multiply(BigDecimal.valueOf(quantity));
        }
    }

    /** Order service: a customer placed an order. */
    record OrderCreated(UUID orderId, String customerId, List<Item> items, BigDecimal total) implements SagaEvent {
    }

    /** Payment service: the customer's account was charged. Carries the items so inventory needn't ask for them. */
    record PaymentCompleted(UUID orderId, String customerId, BigDecimal amount, List<Item> items) implements SagaEvent {
    }

    /** Payment service: the account could not be charged; the saga ends here. */
    record PaymentFailed(UUID orderId, String reason) implements SagaEvent {
    }

    /** Inventory service: every item was reserved; the order can be confirmed. */
    record StockReserved(UUID orderId) implements SagaEvent {
    }

    /** Inventory service: some item is short; the payment must be undone. */
    record StockRejected(UUID orderId, String reason) implements SagaEvent {
    }

    /** Payment service: the compensation of a charge, after the order could not be fulfilled. */
    record PaymentRefunded(UUID orderId, BigDecimal amount, String reason) implements SagaEvent {
    }
}
