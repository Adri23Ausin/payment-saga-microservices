package com.adrianausin.saga.order.domain;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent.Item;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

    private final JdbcClient jdbc;

    OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID id, String customerId, List<Item> items, BigDecimal total, Instant now) {
        jdbc.sql("""
                        INSERT INTO orders (id, customer_id, status, total, created_at, updated_at)
                        VALUES (:id, :customer, 'PENDING', :total, :now, :now)
                        """)
                .param("id", id)
                .param("customer", customerId)
                .param("total", total)
                .param("now", Timestamp.from(now))
                .update();
        for (int line = 0; line < items.size(); line++) {
            Item item = items.get(line);
            jdbc.sql("INSERT INTO order_items (order_id, line, sku, quantity, unit_price) VALUES (:id, :line, :sku, :quantity, :price)")
                    .param("id", id)
                    .param("line", line + 1)
                    .param("sku", item.sku())
                    .param("quantity", item.quantity())
                    .param("price", item.unitPrice())
                    .update();
        }
    }

    /**
     * Moves a pending order to a final status. Conditional on the order still being pending, so a
     * late or repeated event can never overturn an outcome.
     *
     * @return false if the order was not pending
     */
    public boolean complete(UUID id, Order.Status status, @Nullable String reason, Instant now) {
        return jdbc.sql("""
                        UPDATE orders SET status = :status, reason = :reason, updated_at = :now
                        WHERE id = :id AND status = 'PENDING'
                        """)
                .param("id", id)
                .param("status", status.name())
                .param("reason", reason)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    public void addHistory(UUID id, String step, @Nullable String detail, Instant now) {
        jdbc.sql("INSERT INTO order_history (order_id, occurred_at, step, detail) VALUES (:id, :now, :step, :detail)")
                .param("id", id)
                .param("now", Timestamp.from(now))
                .param("step", step)
                .param("detail", detail)
                .update();
    }

    public Optional<Order> findById(UUID id) {
        return jdbc.sql("SELECT * FROM orders WHERE id = :id")
                .param("id", id)
                .query(this::map)
                .optional();
    }

    public List<Order> findByCustomer(String customerId) {
        return jdbc.sql("SELECT * FROM orders WHERE customer_id = :customer ORDER BY created_at DESC LIMIT 50")
                .param("customer", customerId)
                .query(this::map)
                .list();
    }

    private Order map(ResultSet rs, int row) throws SQLException {
        UUID id = rs.getObject("id", UUID.class);
        List<Item> items = jdbc.sql("SELECT sku, quantity, unit_price FROM order_items WHERE order_id = :id ORDER BY line")
                .param("id", id)
                .query((r, n) -> new Item(r.getString("sku"), r.getInt("quantity"), r.getBigDecimal("unit_price")))
                .list();
        List<Order.HistoryEntry> history = jdbc.sql("""
                        SELECT occurred_at, step, detail FROM order_history WHERE order_id = :id ORDER BY occurred_at, step
                        """)
                .param("id", id)
                .query((r, n) -> new Order.HistoryEntry(r.getTimestamp("occurred_at").toInstant(), r.getString("step"),
                        r.getString("detail")))
                .list();
        return new Order(id, rs.getString("customer_id"), Order.Status.valueOf(rs.getString("status")), items,
                rs.getBigDecimal("total"), rs.getString("reason"), rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(), history);
    }
}
