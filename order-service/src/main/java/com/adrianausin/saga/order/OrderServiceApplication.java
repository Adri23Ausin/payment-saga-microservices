package com.adrianausin.saga.order;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        builder().run(args);
    }

    /** Its own config file name, so several services can share a classpath (the end-to-end tests). */
    public static SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(OrderServiceApplication.class).properties("spring.config.name=order-service");
    }
}
