package com.redis.lettucemod.timeseries;

public enum Aggregation {

    AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, STD_P("STD.P"), STD_S("STD.S"), VAR_P("VAR.P"), VAR_S("VAR.S");

    private final String name;

    public String getName() {
        return name;
    }

    Aggregation(String name) {
        this.name = name;
    }

    Aggregation() {
        this.name = this.name();
    }
}
