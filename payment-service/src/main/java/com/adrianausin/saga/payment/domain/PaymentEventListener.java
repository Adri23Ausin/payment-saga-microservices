package com.adrianausin.saga.payment.domain;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.inbox.ProcessedEvents;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The payment service's part of the saga. The event is marked as processed, the account changes
 * and the next event goes to the outbox in one database transaction.
 */
@Component
class PaymentEventListener {

    private final ProcessedEvents processedEvents;
    private final PaymentService payments;

    PaymentEventListener(ProcessedEvents processedEvents, PaymentService payments) {
        this.processedEvents = processedEvents;
        this.payments = payments;
    }

    @KafkaListener(topics = {SagaTopics.ORDERS, SagaTopics.INVENTORY})
    @Transactional
    void on(ConsumerRecord<String, SagaEvent> record) {
        if (!processedEvents.firstDelivery(record)) {
            return;
        }
        switch (record.value()) {
            case SagaEvent.OrderCreated order -> payments.charge(order);
            case SagaEvent.StockRejected rejection -> payments.refund(rejection);
            default -> {
                // Not a step of this service
            }
        }
    }
}
