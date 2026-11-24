package com.adrianausin.saga.payment;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        builder().run(args);
    }

    /** Its own config file name, so several services can share a classpath (the end-to-end tests). */
    public static SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(PaymentServiceApplication.class).properties("spring.config.name=payment-service");
    }
}
