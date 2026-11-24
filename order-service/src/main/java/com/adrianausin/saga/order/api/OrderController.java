package com.adrianausin.saga.order.api;

import java.math.BigDecimal;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import com.adrianausin.saga.common.event.SagaEvent.Item;
import com.adrianausin.saga.order.domain.Order;
import com.adrianausin.saga.order.domain.OrderNotFoundException;
import com.adrianausin.saga.order.domain.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/orders")
class OrderController {

    private final OrderService orders;

    OrderController(OrderService orders) {
        this.orders = orders;
    }

    /** 202: the order is pending until the saga finishes; poll it for the outcome. */
    @PostMapping
    ResponseEntity<Order> place(@Valid @RequestBody PlaceOrder request) {
        Order order = orders.place(request.customerId(), request.items().stream().map(ItemRequest::toItem).toList());
        return ResponseEntity.accepted().location(URI.create("/api/v1/orders/" + order.id())).body(order);
    }

    @GetMapping("/{id}")
    Order get(@PathVariable UUID id) {
        return orders.get(id);
    }

    @GetMapping
    List<Order> byCustomer(@RequestParam String customerId) {
        return orders.findByCustomer(customerId);
    }

    @ExceptionHandler(OrderNotFoundException.class)
    ProblemDetail notFound(OrderNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    record PlaceOrder(@NotBlank @Size(max = 50) String customerId, @NotEmpty @Size(max = 20) List<@Valid ItemRequest> items) {
    }

    record ItemRequest(
            @NotBlank @Size(max = 50) String sku,
            @Positive int quantity,
            @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal unitPrice) {

        Item toItem() {
            return new Item(sku, quantity, unitPrice);
        }
    }
}
