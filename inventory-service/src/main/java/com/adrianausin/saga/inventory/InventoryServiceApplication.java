package com.adrianausin.saga.inventory;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class InventoryServiceApplication {

    public static void main(String[] args) {
        builder().run(args);
    }

    /** Its own config file name, so several services can share a classpath (the end-to-end tests). */
    public static SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(InventoryServiceApplication.class).properties("spring.config.name=inventory-service");
    }
}
