package com.adrianausin.saga.payment.domain;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.adrianausin.saga.common.event.SagaEvent;
import com.adrianausin.saga.common.event.SagaTopics;
import com.adrianausin.saga.common.outbox.Outbox;
import org.springframework.stereotype.Service;

/** Charges orders. Called inside the transaction of the event being handled. */
@Service
public class PaymentService {

    private final PaymentRepository payments;
    private final Outbox outbox;
    private final Clock clock;

    PaymentService(PaymentRepository payments, Outbox outbox, Clock clock) {
        this.payments = payments;
        this.outbox = outbox;
        this.clock = clock;
    }

    public void charge(SagaEvent.OrderCreated order) {
        Instant now = now();
        if (payments.balance(order.customerId()).isEmpty()) {
            decline(order, "Unknown customer " + order.customerId(), now);
            return;
        }
        if (!payments.withdraw(order.customerId(), order.total())) {
            decline(order, "Insufficient funds", now);
            return;
        }
        payments.insert(order.orderId(), order.customerId(), order.total(), Payment.Status.CHARGED, null, now);
        outbox.add(SagaTopics.PAYMENTS,
                new SagaEvent.PaymentCompleted(order.orderId(), order.customerId(), order.total(), order.items()));
    }

    private void decline(SagaEvent.OrderCreated order, String reason, Instant now) {
        payments.insert(order.orderId(), order.customerId(), order.total(), Payment.Status.DECLINED, reason, now);
        outbox.add(SagaTopics.PAYMENTS, new SagaEvent.PaymentFailed(order.orderId(), reason));
    }

    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MICROS);
    }
}
