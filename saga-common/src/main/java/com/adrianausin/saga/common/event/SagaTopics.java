package com.adrianausin.saga.common.event;

/**
 * One topic per publishing service. Records are keyed by order id, so all events of an order land
 * in the same partition and are consumed in the order they were published.
 */
public final class SagaTopics {

    public static final String ORDERS = "saga.orders";
    public static final String PAYMENTS = "saga.payments";
    public static final String INVENTORY = "saga.inventory";

    /** Header with the id of the event, which consumers use to ignore duplicates. */
    public static final String EVENT_ID_HEADER = "event-id";

    private SagaTopics() {
    }
}
