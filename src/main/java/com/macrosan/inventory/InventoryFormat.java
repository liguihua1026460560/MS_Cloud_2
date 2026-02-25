package com.macrosan.inventory;

public enum InventoryFormat {
    CSV("CSV"),
    ORC("ORC"),
    Parquet("Parquet")
    ;

    private final String format;

    InventoryFormat(String format) {
        this.format = format;
    }

    @Override
    public String toString() {
        return format;
    }
}
