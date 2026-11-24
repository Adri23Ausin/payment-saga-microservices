package com.adrianausin.saga.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaEvent.Item;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.testing.SagaContainers;
import com.adrianausin.saga.common.testing.SagaRecords;
import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

@SpringBootTest(properties = "spring.config.name=order-service")
@AutoConfigureMockMvc
@Import(SagaContainers.class)
class OrderPlacementIT {

    @Autowired
    private MockMvcTester mvc;

    @Test
    void placingAnOrderStoresItAsPendingAndStartsTheSaga() {
        MvcTestResult result = mvc.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
                {"customerId": "alice", "items": [
                  {"sku": "KEYBOARD", "quantity": 1, "unitPrice": 89.90},
                  {"sku": "MOUSE", "quantity": 2, "unitPrice": 24.50}]}""").exchange();

        assertThat(result).hasStatus(HttpStatus.ACCEPTED).bodyJson()
                .hasPathSatisfying("$.status", status -> status.assertThat().isEqualTo("PENDING"))
                .hasPathSatisfying("$.total", total -> total.assertThat().isEqualTo(138.90))
                .hasPathSatisfying("$.history[0].step", step -> step.assertThat().isEqualTo("ORDER_PLACED"));
        UUID orderId = UUID.fromString(JsonPath.read(body(result), "$.id"));
        assertThat(result.getResponse().getHeader("Location")).isEqualTo("/api/v1/orders/" + orderId);

        try (KafkaConsumer<String, String> consumer = SagaRecords.consumer(SagaTopics.ORDERS)) {
            List<SagaEvent> events = SagaRecords.newList();
            await().atMost(Duration.ofSeconds(20)).until(() -> !SagaRecords.eventsOf(consumer, orderId, events).isEmpty());
            assertThat(events).containsExactly(new SagaEvent.OrderCreated(orderId, "alice", List.of(
                    new Item("KEYBOARD", 1, new BigDecimal("89.90")),
                    new Item("MOUSE", 2, new BigDecimal("24.50"))), new BigDecimal("138.90")));
        }
    }

    @Test
    void amountsAlwaysTravelWithTwoDecimals() {
        MvcTestResult result = place("""
                {"customerId": "alice", "items": [{"sku": "KEYBOARD", "quantity": 1, "unitPrice": 89.9}]}""");
        UUID orderId = UUID.fromString(JsonPath.read(body(result), "$.id"));

        try (KafkaConsumer<String, String> consumer = SagaRecords.consumer(SagaTopics.ORDERS)) {
            List<SagaEvent> events = SagaRecords.newList();
            await().atMost(Duration.ofSeconds(20)).until(() -> !SagaRecords.eventsOf(consumer, orderId, events).isEmpty());
            SagaEvent.OrderCreated created = (SagaEvent.OrderCreated) events.getFirst();
            assertThat(created.total()).hasToString("89.90");
            assertThat(created.items().getFirst().unitPrice()).hasToString("89.90");
        }
    }

    @Test
    void ordersCanBeReadAndListedByCustomer() {
        String customer = "customer-" + UUID.randomUUID();
        UUID orderId = UUID.fromString(JsonPath.read(body(mvc.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"customerId": "%s", "items": [{"sku": "MOUSE", "quantity": 1, "unitPrice": 24.50}]}""".formatted(customer))
                .exchange()), "$.id"));

        assertThat(mvc.get().uri("/api/v1/orders/{id}", orderId)).hasStatusOk().bodyJson()
                .hasPathSatisfying("$.items[0].sku", sku -> sku.assertThat().isEqualTo("MOUSE"));
        assertThat(mvc.get().uri("/api/v1/orders?customerId={customer}", customer)).hasStatusOk().bodyJson()
                .hasPathSatisfying("$[*].id", ids -> ids.assertThat().asArray().containsExactly(orderId.toString()));
        assertThat(mvc.get().uri("/api/v1/orders/{id}", UUID.randomUUID())).hasStatus(HttpStatus.NOT_FOUND);
    }

    @Test
    void invalidOrdersAreRejected() {
        assertThat(place("""
                {"customerId": "alice", "items": []}""")).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(place("""
                {"customerId": "alice", "items": [{"sku": "MOUSE", "quantity": 0, "unitPrice": 24.50}]}""")).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(place("""
                {"customerId": "alice", "items": [{"sku": "MOUSE", "quantity": 1, "unitPrice": 24.505}]}""")).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(place("""
                {"customerId": " ", "items": [{"sku": "MOUSE", "quantity": 1, "unitPrice": 24.50}]}""")).hasStatus(HttpStatus.BAD_REQUEST);
    }

    private MvcTestResult place(String body) {
        return mvc.post().uri("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(body).exchange();
    }

    private static String body(MvcTestResult result) {
        return new String(result.getResponse().getContentAsByteArray(), StandardCharsets.UTF_8);
    }
}
