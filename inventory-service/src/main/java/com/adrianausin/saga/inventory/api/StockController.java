package com.adrianausin.saga.inventory.api;

import java.util.Map;

import com.adrianausin.saga.inventory.domain.InventoryService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/stock")
class StockController {

    private final InventoryService inventory;

    StockController(InventoryService inventory) {
        this.inventory = inventory;
    }

    /** Available quantity per SKU. */
    @GetMapping
    Map<String, Integer> stock() {
        return inventory.stock();
    }

    @PutMapping("/{sku}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void restock(@PathVariable String sku, @Valid @RequestBody Restock request) {
        inventory.restock(sku, request.available());
    }

    record Restock(@PositiveOrZero int available) {
    }
}
