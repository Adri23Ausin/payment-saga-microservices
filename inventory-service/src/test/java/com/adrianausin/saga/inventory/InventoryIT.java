package com.adrianausin.saga.inventory;

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
import com.adrianausin.saga.inventory.domain.InventoryService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = "spring.config.name=inventory-service")
@Import(SagaContainers.class)
class InventoryIT {

    @Autowired
    private InventoryService inventory;

    private KafkaConsumer<String, String> inventoryEvents;
    private String lamp;
    private String desk;

    @BeforeEach
    void setUp() {
        lamp = "LAMP-" + UUID.randomUUID();
        desk = "DESK-" + UUID.randomUUID();
        inventory.restock(lamp, 5);
        inventory.restock(desk, 1);
        inventoryEvents = SagaRecords.consumer(SagaTopics.INVENTORY);
    }

    @AfterEach
    void tearDown() {
        inventoryEvents.close();
    }

    @Test
    void aPaidOrderReservesItsStock() {
        UUID orderId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.PAYMENTS, paid(orderId, item(lamp, 2), item(desk, 1)), UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(new SagaEvent.StockReserved(orderId));
        assertThat(inventory.stock()).containsEntry(lamp, 3).containsEntry(desk, 0);
    }

    @Test
    void ifAnyItemIsShortNothingIsReserved() {
        UUID orderId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.PAYMENTS, paid(orderId, item(lamp, 2), item(desk, 2)), UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(new SagaEvent.StockRejected(orderId, desk + ": 2 requested, 1 available"));
        assertThat(inventory.stock()).containsEntry(lamp, 5).containsEntry(desk, 1);
    }

    @Test
    void theSameProductOnSeveralLinesIsCountedTogether() {
        UUID orderId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.PAYMENTS, paid(orderId, item(lamp, 3), item(lamp, 3)), UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(new SagaEvent.StockRejected(orderId, lamp + ": 6 requested, 5 available"));
    }

    @Test
    void unknownProductsAreShort() {
        UUID orderId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.PAYMENTS, paid(orderId, item("NO-SUCH-SKU", 1)), UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(new SagaEvent.StockRejected(orderId, "NO-SUCH-SKU: unknown product"));
    }

    @Test
    void aRedeliveredPaymentReservesOnce() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.PAYMENTS, paid(orderId, item(lamp, 2)), eventId);
        SagaRecords.publish(SagaTopics.PAYMENTS, paid(orderId, item(lamp, 2)), eventId);
        awaitEvent(orderId);
        UUID nextOrder = UUID.randomUUID();
        SagaRecords.publish(SagaTopics.PAYMENTS, paid(nextOrder, item(lamp, 1)), UUID.randomUUID());
        awaitEvent(nextOrder);

        assertThat(inventory.stock()).containsEntry(lamp, 2);
    }

    private static Item item(String sku, int quantity) {
        return new Item(sku, quantity, new BigDecimal("10.00"));
    }

    private static SagaEvent.PaymentCompleted paid(UUID orderId, Item... items) {
        List<Item> lines = List.of(items);
        BigDecimal total = lines.stream().map(Item::total).reduce(BigDecimal.ZERO, BigDecimal::add);
        return new SagaEvent.PaymentCompleted(orderId, "alice", total, lines);
    }

    private SagaEvent awaitEvent(UUID orderId) {
        List<SagaEvent> events = SagaRecords.newList();
        await().atMost(Duration.ofSeconds(20)).until(() -> !SagaRecords.eventsOf(inventoryEvents, orderId, events).isEmpty());
        return events.getFirst();
    }
}
