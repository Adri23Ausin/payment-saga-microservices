package com.adrianausin.saga.common.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent.Item;
import org.junit.jupiter.api.Test;

class SagaEventSerdeTest {

    private static final UUID ORDER = UUID.fromString("0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b");

    @Test
    void everyEventSurvivesARoundTrip() {
        List<Item> items = List.of(new Item("SKU-1", 2, new BigDecimal("9.99")));
        List<SagaEvent> events = List.of(
                new SagaEvent.OrderCreated(ORDER, "alice", items, new BigDecimal("19.98")),
                new SagaEvent.PaymentCompleted(ORDER, "alice", new BigDecimal("19.98"), items),
                new SagaEvent.PaymentFailed(ORDER, "Insufficient funds"),
                new SagaEvent.StockReserved(ORDER),
                new SagaEvent.StockRejected(ORDER, "SKU-1: 2 requested, 1 available"),
                new SagaEvent.PaymentRefunded(ORDER, new BigDecimal("19.98"), "Out of stock"));

        assertThat(events).allSatisfy(event -> assertThat(SagaEventSerde.fromJson(SagaEventSerde.toJson(event))).isEqualTo(event));
    }

    @Test
    void theJsonNamesTheEventType() {
        assertThat(SagaEventSerde.toJson(new SagaEvent.StockReserved(ORDER)))
                .isEqualTo("{\"type\":\"StockReserved\",\"orderId\":\"" + ORDER + "\"}");
    }

    @Test
    void unknownTypesAreRejected() {
        assertThatThrownBy(() -> SagaEventSerde.fromJson("{\"type\":\"OrderTeleported\",\"orderId\":\"" + ORDER + "\"}"))
                .hasMessageContaining("OrderTeleported");
    }

    @Test
    void itemTotalsMultiplyPriceAndQuantity() {
        assertThat(new Item("SKU-1", 3, new BigDecimal("4.50")).total()).isEqualByComparingTo("13.50");
    }
}
