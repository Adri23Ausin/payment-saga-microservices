package com.adrianausin.saga.order.domain;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.Clock;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.inbox.ProcessedEvents;
import org.jspecify.annotations.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Follows the saga of each order and decides how it ends: confirmed once stock is reserved,
 * cancelled when the payment fails or after a refund. Every step is recorded in the order's history.
 * Events come from two topics and can interleave, so only a pending order changes status.
 */
@Component
class OrderSagaListener {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaListener.class);

    private final ProcessedEvents processedEvents;
    private final OrderRepository orders;
    private final Clock clock;

    OrderSagaListener(ProcessedEvents processedEvents, OrderRepository orders, Clock clock) {
        this.processedEvents = processedEvents;
        this.orders = orders;
        this.clock = clock;
    }

    @KafkaListener(topics = {SagaTopics.PAYMENTS, SagaTopics.INVENTORY})
    @Transactional
    void on(ConsumerRecord<String, SagaEvent> record) {
        SagaEvent event = record.value();
        if (!processedEvents.firstDelivery(record)) {
            return;
        }
        if (orders.findById(event.orderId()).isEmpty()) {
            log.warn("Ignoring {} for unknown order {}", event.getClass().getSimpleName(), event.orderId());
            return;
        }
        switch (event) {
            case SagaEvent.PaymentCompleted paid -> step(paid.orderId(), "PAYMENT_COMPLETED", "EUR " + paid.amount() + " charged");
            case SagaEvent.PaymentFailed failed -> {
                step(failed.orderId(), "PAYMENT_FAILED", failed.reason());
                complete(failed.orderId(), Order.Status.CANCELLED, failed.reason());
            }
            case SagaEvent.StockReserved reserved -> {
                step(reserved.orderId(), "STOCK_RESERVED", null);
                complete(reserved.orderId(), Order.Status.CONFIRMED, null);
            }
            // The payment service reacts with a refund; the order is cancelled when that arrives
            case SagaEvent.StockRejected rejected -> step(rejected.orderId(), "STOCK_REJECTED", rejected.reason());
            case SagaEvent.PaymentRefunded refunded -> {
                step(refunded.orderId(), "PAYMENT_REFUNDED", "EUR " + refunded.amount() + " refunded");
                complete(refunded.orderId(), Order.Status.CANCELLED, "Out of stock (" + refunded.reason() + "), payment refunded");
            }
            case SagaEvent.OrderCreated created -> {
                // Published by this service
            }
        }
    }

    private void step(UUID orderId, String step, @Nullable String detail) {
        orders.addHistory(orderId, step, detail, now());
    }

    private void complete(UUID orderId, Order.Status status, @Nullable String reason) {
        if (!orders.complete(orderId, status, reason, now())) {
            log.warn("Order {} was already completed, not moving it to {}", orderId, status);
        }
    }

    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MICROS);
    }
}
