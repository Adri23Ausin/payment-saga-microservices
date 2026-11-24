package com.adrianausin.saga.order.domain;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaEvent.Item;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.outbox.Outbox;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final Outbox outbox;
    private final Clock clock;

    OrderService(OrderRepository orders, Outbox outbox, Clock clock) {
        this.orders = orders;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * Stores the order as pending and starts its saga. The order and its OrderCreated event commit
     * together: there is never an order nobody hears about, nor an event about an order that isn't there.
     */
    @Transactional
    public Order place(String customerId, List<Item> items) {
        UUID id = UUID.randomUUID();
        BigDecimal total = Order.totalOf(items);
        Instant now = now();
        orders.insert(id, customerId, items, total, now);
        orders.addHistory(id, "ORDER_PLACED", null, now);
        outbox.add(SagaTopics.ORDERS, new SagaEvent.OrderCreated(id, customerId, items, total));
        return orders.findById(id).orElseThrow();
    }

    public Order get(UUID id) {
        return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    public List<Order> findByCustomer(String customerId) {
        return orders.findByCustomer(customerId);
    }

    /** PostgreSQL keeps microseconds: responses must show the same instants as later reads. */
    Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MICROS);
    }
}
