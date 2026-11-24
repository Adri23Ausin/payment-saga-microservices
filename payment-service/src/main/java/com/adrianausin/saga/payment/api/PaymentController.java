package com.adrianausin.saga.payment.api;

import java.math.BigDecimal;
import java.util.UUID;

import com.adrianausin.saga.payment.domain.Payment;
import com.adrianausin.saga.payment.domain.PaymentRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
class PaymentController {

    private final PaymentRepository payments;

    PaymentController(PaymentRepository payments) {
        this.payments = payments;
    }

    @GetMapping("/accounts/{customerId}")
    ResponseEntity<Account> account(@PathVariable String customerId) {
        return ResponseEntity.of(payments.balance(customerId).map(balance -> new Account(customerId, balance)));
    }

    @GetMapping("/payments/{orderId}")
    ResponseEntity<Payment> payment(@PathVariable UUID orderId) {
        return ResponseEntity.of(payments.findByOrder(orderId));
    }

    record Account(String customerId, BigDecimal balance) {
    }
}
