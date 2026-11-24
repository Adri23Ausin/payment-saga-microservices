package com.adrianausin.saga.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaEvent.Item;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.testing.SagaContainers;
import com.adrianausin.saga.common.testing.SagaRecords;
import com.adrianausin.saga.order.domain.Order;
import com.adrianausin.saga.order.domain.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/** The order service reacting to the other services' events, published here by hand. */
@SpringBootTest(properties = "spring.config.name=order-service")
@Import(SagaContainers.class)
class OrderSagaIT {

    private static final List<Item> ITEMS = List.of(new Item("MONITOR", 1, new BigDecimal("199.00")));

    @Autowired
    private OrderService orders;

    @Test
    void anOrderIsConfirmedOnceItsStockIsReserved() {
        UUID orderId = orders.place("alice", ITEMS).id();

        publish(new SagaEvent.PaymentCompleted(orderId, "alice", new BigDecimal("199.00"), ITEMS));
        publish(new SagaEvent.StockReserved(orderId));

        Order order = awaitFinal(orderId);
        assertThat(order.status()).isEqualTo(Order.Status.CONFIRMED);
        assertThat(order.history()).extracting(Order.HistoryEntry::step)
                .containsExactly("ORDER_PLACED", "PAYMENT_COMPLETED", "STOCK_RESERVED");
    }

    @Test
    void anOrderIsCancelledWhenThePaymentFails() {
        UUID orderId = orders.place("alice", ITEMS).id();

        publish(new SagaEvent.PaymentFailed(orderId, "Insufficient funds"));

        Order order = awaitFinal(orderId);
        assertThat(order.status()).isEqualTo(Order.Status.CANCELLED);
        assertThat(order.reason()).isEqualTo("Insufficient funds");
    }

    @Test
    void anOrderOutOfStockIsCancelledWhenThePaymentIsRefunded() {
        UUID orderId = orders.place("alice", ITEMS).id();

        publish(new SagaEvent.PaymentCompleted(orderId, "alice", new BigDecimal("199.00"), ITEMS));
        publish(new SagaEvent.StockRejected(orderId, "MONITOR: 1 requested, 0 available"));
        await().atMost(Duration.ofSeconds(20)).until(() -> orders.get(orderId).history().size() == 3);
        assertThat(orders.get(orderId).status()).isEqualTo(Order.Status.PENDING);

        publish(new SagaEvent.PaymentRefunded(orderId, new BigDecimal("199.00"), "MONITOR: 1 requested, 0 available"));

        Order order = awaitFinal(orderId);
        assertThat(order.status()).isEqualTo(Order.Status.CANCELLED);
        assertThat(order.reason()).isEqualTo("Out of stock (MONITOR: 1 requested, 0 available), payment refunded");
        assertThat(order.history()).extracting(Order.HistoryEntry::step)
                .containsExactly("ORDER_PLACED", "PAYMENT_COMPLETED", "STOCK_REJECTED", "PAYMENT_REFUNDED");
    }

    @Test
    void aLateEventDoesNotOverturnTheOutcome() {
        UUID orderId = orders.place("alice", ITEMS).id();
        publish(new SagaEvent.PaymentFailed(orderId, "Insufficient funds"));
        awaitFinal(orderId);

        publish(new SagaEvent.StockReserved(orderId));

        await().atMost(Duration.ofSeconds(20)).until(() -> orders.get(orderId).history().size() == 3);
        assertThat(orders.get(orderId).status()).isEqualTo(Order.Status.CANCELLED);
    }

    @Test
    void eventsForUnknownOrdersAreIgnored() {
        UUID unknown = UUID.randomUUID();
        publish(new SagaEvent.StockReserved(unknown));
        UUID orderId = orders.place("alice", ITEMS).id();
        publish(new SagaEvent.PaymentFailed(orderId, "Insufficient funds"));

        assertThat(awaitFinal(orderId).status()).isEqualTo(Order.Status.CANCELLED);
    }

    private static void publish(SagaEvent event) {
        String topic = switch (event) {
            case SagaEvent.StockReserved ignored -> SagaTopics.INVENTORY;
            case SagaEvent.StockRejected ignored -> SagaTopics.INVENTORY;
            default -> SagaTopics.PAYMENTS;
        };
        SagaRecords.publish(topic, event, UUID.randomUUID());
    }

    private Order awaitFinal(UUID orderId) {
        await().atMost(Duration.ofSeconds(20)).until(() -> orders.get(orderId).status().isFinal());
        return orders.get(orderId);
    }
}
