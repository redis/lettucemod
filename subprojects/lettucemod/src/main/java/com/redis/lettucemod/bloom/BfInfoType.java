package com.redis.lettucemod.bloom;

public enum BfInfoType {
    CAPACITY,
    SIZE,
    FILTERS,
    ITEMS,
    EXPANSION;


    private final String name;

    BfInfoType(){this.name = this.name();}
}
