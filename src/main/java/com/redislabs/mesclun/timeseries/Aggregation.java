package com.redislabs.mesclun.timeseries;

import lombok.Getter;

public enum Aggregation {

    AVG,
    SUM,
    MIN,
    MAX,
    RANGE,
    COUNT,
    FIRST,
    LAST,
    STD_P("STD.P"),
    STD_S("STD.S"),
    VAR_P("VAR.P"),
    VAR_S("VAR.S");

    @Getter
    private final String name;

    Aggregation(String name) {
        this.name = name;
    }

    Aggregation() {
        this.name = this.name();
    }
}
