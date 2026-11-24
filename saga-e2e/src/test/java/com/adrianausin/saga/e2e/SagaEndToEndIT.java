package com.adrianausin.saga.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import com.adrianausin.saga.common.testing.SagaContainers;
import com.adrianausin.saga.inventory.InventoryServiceApplication;
import com.adrianausin.saga.order.OrderServiceApplication;
import com.adrianausin.saga.payment.PaymentServiceApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

/**
 * The three services, each with its own database, running together against one Kafka broker.
 * They only talk through events; the test only talks to their REST APIs.
 */
class SagaEndToEndIT {

    private static final ParameterizedTypeReference<Map<String, Object>> JSON = new ParameterizedTypeReference<>() {
    };

    private static ConfigurableApplicationContext orderService;
    private static ConfigurableApplicationContext paymentService;
    private static ConfigurableApplicationContext inventoryService;
    private static RestClient orders;
    private static RestClient payments;
    private static RestClient inventory;

    @BeforeAll
    static void startServices() throws SQLException {
        SagaContainers.POSTGRES.start();
        SagaContainers.KAFKA.start();
        createDatabases("orders", "payments", "inventory");

        orderService = start(OrderServiceApplication.builder(), "orders");
        paymentService = start(PaymentServiceApplication.builder(), "payments");
        inventoryService = start(InventoryServiceApplication.builder(), "inventory");
        orders = client(orderService);
        payments = client(paymentService);
        inventory = client(inventoryService);
    }

    @AfterAll
    static void stopServices() {
        List.of(orderService, paymentService, inventoryService).forEach(ConfigurableApplicationContext::close);
    }

    @Test
    void anAffordableOrderInStockIsConfirmed() {
        String sku = newProduct(10);
        BigDecimal before = balance("alice");

        Map<String, Object> order = awaitOutcome(place("alice", sku, 2, "45.00"));

        assertThat(order.get("status")).isEqualTo("CONFIRMED");
        assertThat(steps(order)).containsExactly("ORDER_PLACED", "PAYMENT_COMPLETED", "STOCK_RESERVED");
        assertThat(balance("alice")).isEqualByComparingTo(before.subtract(new BigDecimal("90.00")));
        assertThat(stock(sku)).isEqualTo(8);
    }

    @Test
    void anOrderAboveTheBalanceIsCancelledWithoutTouchingTheStock() {
        String sku = newProduct(10);

        Map<String, Object> order = awaitOutcome(place("bob", sku, 1, "500.00"));

        assertThat(order.get("status")).isEqualTo("CANCELLED");
        assertThat(order.get("reason")).isEqualTo("Insufficient funds");
        assertThat(steps(order)).containsExactly("ORDER_PLACED", "PAYMENT_FAILED");
        assertThat(stock(sku)).isEqualTo(10);
    }

    @Test
    void anOrderOutOfStockIsRefundedAndCancelled() {
        String sku = newProduct(0);
        BigDecimal before = balance("carol");

        Map<String, Object> order = awaitOutcome(place("carol", sku, 1, "60.00"));

        assertThat(order.get("status")).isEqualTo("CANCELLED");
        assertThat((String) order.get("reason")).startsWith("Out of stock").endsWith("payment refunded");
        assertThat(steps(order)).containsExactly("ORDER_PLACED", "PAYMENT_COMPLETED", "STOCK_REJECTED", "PAYMENT_REFUNDED");
        assertThat(balance("carol")).isEqualByComparingTo(before);
        assertThat(payments.get().uri("/api/v1/payments/{id}", order.get("id")).retrieve().body(JSON))
                .containsEntry("status", "REFUNDED");
    }

    @Test
    void concurrentOrdersForTheLastUnitsNeverOversell() {
        String sku = newProduct(2);
        BigDecimal before = balance("alice");

        List<CompletableFuture<String>> placed = IntStream.range(0, 5)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> place("alice", sku, 1, "10.00")))
                .toList();
        List<Map<String, Object>> outcomes = new ArrayList<>();
        placed.forEach(id -> outcomes.add(awaitOutcome(id.join())));

        assertThat(outcomes).filteredOn(order -> order.get("status").equals("CONFIRMED")).hasSize(2);
        assertThat(outcomes).filteredOn(order -> order.get("status").equals("CANCELLED")).hasSize(3);
        assertThat(stock(sku)).isZero();
        assertThat(balance("alice")).isEqualByComparingTo(before.subtract(new BigDecimal("20.00")));
    }

    @Test
    void theSagaResumesWhenAServiceComesBack() {
        String sku = newProduct(5);
        inventoryService.close();
        try {
            String id = place("alice", sku, 1, "15.00");
            await().atMost(Duration.ofSeconds(30)).until(() -> steps(order(id)).contains("PAYMENT_COMPLETED"));
            assertThat(order(id).get("status")).isEqualTo("PENDING");

            inventoryService = start(InventoryServiceApplication.builder(), "inventory");
            inventory = client(inventoryService);

            assertThat(awaitOutcome(id).get("status")).isEqualTo("CONFIRMED");
            assertThat(stock(sku)).isEqualTo(4);
        }
        finally {
            if (!inventoryService.isActive()) {
                inventoryService = start(InventoryServiceApplication.builder(), "inventory");
                inventory = client(inventoryService);
            }
        }
    }

    private static String place(String customer, String sku, int quantity, String unitPrice) {
        Map<String, Object> order = orders.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("customerId", customer, "items", List.of(Map.of("sku", sku, "quantity", quantity, "unitPrice", unitPrice))))
                .retrieve()
                .body(JSON);
        return (String) order.get("id");
    }

    private static Map<String, Object> order(String id) {
        return orders.get().uri("/api/v1/orders/{id}", id).retrieve().body(JSON);
    }

    private static Map<String, Object> awaitOutcome(String id) {
        await().atMost(Duration.ofSeconds(60)).until(() -> !order(id).get("status").equals("PENDING"));
        return order(id);
    }

    @SuppressWarnings("unchecked")
    private static List<String> steps(Map<String, Object> order) {
        return ((List<Map<String, Object>>) order.get("history")).stream().map(entry -> (String) entry.get("step")).toList();
    }

    private static BigDecimal balance(String customer) {
        return new BigDecimal(payments.get().uri("/api/v1/accounts/{id}", customer).retrieve().body(JSON).get("balance").toString());
    }

    private static String newProduct(int available) {
        String sku = "SKU-" + UUID.randomUUID();
        inventory.put().uri("/api/v1/stock/{sku}", sku).contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("available", available)).retrieve().toBodilessEntity();
        return sku;
    }

    private static int stock(String sku) {
        return ((Number) inventory.get().uri("/api/v1/stock").retrieve().body(JSON).get(sku)).intValue();
    }

    private static ConfigurableApplicationContext start(SpringApplicationBuilder service, String database) {
        String url = SagaContainers.POSTGRES.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + database + "$1");
        // Command-line arguments: they take precedence over each service's own config file
        return service.run(
                "--server.port=0",
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + SagaContainers.POSTGRES.getUsername(),
                "--spring.datasource.password=" + SagaContainers.POSTGRES.getPassword(),
                "--spring.kafka.bootstrap-servers=" + SagaContainers.KAFKA.getBootstrapServers());
    }

    private static RestClient client(ConfigurableApplicationContext service) {
        return RestClient.create("http://localhost:" + service.getEnvironment().getProperty("local.server.port"));
    }

    private static void createDatabases(String... names) throws SQLException {
        try (Connection connection = DriverManager.getConnection(SagaContainers.POSTGRES.getJdbcUrl(),
                SagaContainers.POSTGRES.getUsername(), SagaContainers.POSTGRES.getPassword());
             Statement statement = connection.createStatement()) {
            for (String name : names) {
                statement.execute("CREATE DATABASE " + name);
            }
        }
    }
}
