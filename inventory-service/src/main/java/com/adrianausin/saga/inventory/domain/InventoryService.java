package com.adrianausin.saga.inventory.domain;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.outbox.Outbox;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

/** Reserves stock for paid orders, all or nothing. Called inside the transaction of the event being handled. */
@Service
public class InventoryService {

    private final JdbcClient jdbc;
    private final Outbox outbox;
    private final Clock clock;

    InventoryService(JdbcClient jdbc, Outbox outbox, Clock clock) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.clock = clock;
    }

    public void reserve(SagaEvent.PaymentCompleted payment) {
        // Sorted by SKU: concurrent orders lock rows in the same order and can't deadlock
        Map<String, Integer> wanted = new TreeMap<>();
        payment.items().forEach(item -> wanted.merge(item.sku(), item.quantity(), Integer::sum));

        List<String> shortages = new ArrayList<>();
        for (Map.Entry<String, Integer> line : wanted.entrySet()) {
            Integer available = jdbc.sql("SELECT available FROM stock WHERE sku = :sku FOR UPDATE")
                    .param("sku", line.getKey())
                    .query(Integer.class)
                    .optional()
                    .orElse(null);
            if (available == null) {
                shortages.add(line.getKey() + ": unknown product");
            }
            else if (available < line.getValue()) {
                shortages.add("%s: %d requested, %d available".formatted(line.getKey(), line.getValue(), available));
            }
        }
        if (!shortages.isEmpty()) {
            outbox.add(SagaTopics.INVENTORY, new SagaEvent.StockRejected(payment.orderId(), String.join("; ", shortages)));
            return;
        }

        Timestamp now = Timestamp.from(clock.instant());
        wanted.forEach((sku, quantity) -> {
            jdbc.sql("UPDATE stock SET available = available - :quantity WHERE sku = :sku")
                    .param("sku", sku)
                    .param("quantity", quantity)
                    .update();
            jdbc.sql("INSERT INTO reservations (order_id, sku, quantity, reserved_at) VALUES (:order, :sku, :quantity, :now)")
                    .param("order", payment.orderId())
                    .param("sku", sku)
                    .param("quantity", quantity)
                    .param("now", now)
                    .update();
        });
        outbox.add(SagaTopics.INVENTORY, new SagaEvent.StockReserved(payment.orderId()));
    }

    public Map<String, Integer> stock() {
        Map<String, Integer> stock = new TreeMap<>();
        jdbc.sql("SELECT sku, available FROM stock").query(rs -> {
            stock.put(rs.getString("sku"), rs.getInt("available"));
        });
        return stock;
    }

    /** Sets the available quantity of a product, adding it if it is new. */
    public void restock(String sku, int available) {
        jdbc.sql("""
                        INSERT INTO stock (sku, available) VALUES (:sku, :available)
                        ON CONFLICT (sku) DO UPDATE SET available = EXCLUDED.available
                        """)
                .param("sku", sku)
                .param("available", available)
                .update();
    }
}
