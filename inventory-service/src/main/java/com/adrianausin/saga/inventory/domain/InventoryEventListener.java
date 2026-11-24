package com.adrianausin.saga.inventory.domain;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.inbox.ProcessedEvents;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** The inventory service's part of the saga: stock is only reserved once an order is paid. */
@Component
class InventoryEventListener {

    private final ProcessedEvents processedEvents;
    private final InventoryService inventory;

    InventoryEventListener(ProcessedEvents processedEvents, InventoryService inventory) {
        this.processedEvents = processedEvents;
        this.inventory = inventory;
    }

    @KafkaListener(topics = SagaTopics.PAYMENTS)
    @Transactional
    void on(ConsumerRecord<String, SagaEvent> record) {
        if (!processedEvents.firstDelivery(record)) {
            return;
        }
        if (record.value() instanceof SagaEvent.PaymentCompleted payment) {
            inventory.reserve(payment);
        }
    }
}
