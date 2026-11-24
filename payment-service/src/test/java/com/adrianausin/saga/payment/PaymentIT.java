package com.adrianausin.saga.payment;

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
import com.adrianausin.saga.payment.domain.Payment;
import com.adrianausin.saga.payment.domain.PaymentRepository;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest(properties = "spring.config.name=payment-service")
@Import(SagaContainers.class)
class PaymentIT {

    private static final List<Item> ITEMS = List.of(new Item("KEYBOARD", 1, new BigDecimal("80.00")));

    @Autowired
    private PaymentRepository payments;
    @Autowired
    private JdbcClient jdbc;

    private KafkaConsumer<String, String> paymentEvents;
    private String customer;

    @BeforeEach
    void setUp() {
        customer = "customer-" + UUID.randomUUID();
        jdbc.sql("INSERT INTO accounts (customer_id, balance) VALUES (:customer, 100.00)").param("customer", customer).update();
        paymentEvents = SagaRecords.consumer(SagaTopics.PAYMENTS);
    }

    @AfterEach
    void tearDown() {
        paymentEvents.close();
    }

    @Test
    void anOrderTheCustomerCanAffordIsCharged() {
        UUID orderId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(orderId, "80.00"), UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(new SagaEvent.PaymentCompleted(orderId, customer, new BigDecimal("80.00"), ITEMS));
        assertThat(payments.balance(customer)).hasValueSatisfying(balance -> assertThat(balance).isEqualByComparingTo("20.00"));
        assertThat(payments.findByOrder(orderId)).hasValueSatisfying(payment -> assertThat(payment.status()).isEqualTo(Payment.Status.CHARGED));
    }

    @Test
    void anOrderAboveTheBalanceIsDeclinedAndNothingIsCharged() {
        UUID orderId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(orderId, "100.01"), UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(new SagaEvent.PaymentFailed(orderId, "Insufficient funds"));
        assertThat(payments.balance(customer)).hasValueSatisfying(balance -> assertThat(balance).isEqualByComparingTo("100.00"));
        assertThat(payments.findByOrder(orderId)).hasValueSatisfying(payment -> assertThat(payment.status()).isEqualTo(Payment.Status.DECLINED));
    }

    @Test
    void unknownCustomersAreDeclined() {
        UUID orderId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.ORDERS, new SagaEvent.OrderCreated(orderId, "nobody", ITEMS, new BigDecimal("80.00")),
                UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(new SagaEvent.PaymentFailed(orderId, "Unknown customer nobody"));
    }

    @Test
    void aRedeliveredEventIsChargedOnce() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(orderId, "30.00"), eventId);
        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(orderId, "30.00"), eventId);
        awaitEvent(orderId);
        // A later order of the same customer proves both deliveries were consumed
        UUID nextOrder = UUID.randomUUID();
        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(nextOrder, "1.00"), UUID.randomUUID());
        awaitEvent(nextOrder);

        assertThat(payments.balance(customer)).hasValueSatisfying(balance -> assertThat(balance).isEqualByComparingTo("69.00"));
    }

    @Test
    void anOrderThatCannotBeFulfilledIsRefundedOnce() {
        UUID orderId = UUID.randomUUID();
        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(orderId, "80.00"), UUID.randomUUID());
        awaitEvent(orderId);

        UUID rejection = UUID.randomUUID();
        SagaRecords.publish(SagaTopics.INVENTORY, new SagaEvent.StockRejected(orderId, "KEYBOARD: 1 requested, 0 available"), rejection);
        SagaRecords.publish(SagaTopics.INVENTORY, new SagaEvent.StockRejected(orderId, "KEYBOARD: 1 requested, 0 available"),
                UUID.randomUUID());

        assertThat(awaitEvent(orderId)).isEqualTo(
                new SagaEvent.PaymentRefunded(orderId, new BigDecimal("80.00"), "KEYBOARD: 1 requested, 0 available"));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(payments.findByOrder(orderId))
                .hasValueSatisfying(payment -> assertThat(payment.status()).isEqualTo(Payment.Status.REFUNDED)));
        assertThat(payments.balance(customer)).hasValueSatisfying(balance -> assertThat(balance).isEqualByComparingTo("100.00"));
    }

    @Test
    void aDeclinedPaymentIsNotRefunded() {
        UUID orderId = UUID.randomUUID();
        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(orderId, "500.00"), UUID.randomUUID());
        awaitEvent(orderId);

        SagaRecords.publish(SagaTopics.INVENTORY, new SagaEvent.StockRejected(orderId, "anything"), UUID.randomUUID());
        UUID nextOrder = UUID.randomUUID();
        SagaRecords.publish(SagaTopics.ORDERS, orderCreated(nextOrder, "1.00"), UUID.randomUUID());
        awaitEvent(nextOrder);

        assertThat(payments.balance(customer)).hasValueSatisfying(balance -> assertThat(balance).isEqualByComparingTo("99.00"));
        assertThat(payments.findByOrder(orderId)).hasValueSatisfying(payment -> assertThat(payment.status()).isEqualTo(Payment.Status.DECLINED));
    }

    private SagaEvent.OrderCreated orderCreated(UUID orderId, String total) {
        return new SagaEvent.OrderCreated(orderId, customer, ITEMS, new BigDecimal(total));
    }

    private SagaEvent awaitEvent(UUID orderId) {
        List<SagaEvent> events = SagaRecords.newList();
        await().atMost(Duration.ofSeconds(20)).until(() -> !SagaRecords.eventsOf(paymentEvents, orderId, events).isEmpty());
        return events.getFirst();
    }
}
