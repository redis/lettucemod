package com.redislabs.lettucemod.search;

import java.util.ArrayList;
import java.util.Map;

public class AggregateResults<K> extends ArrayList<Map<K, Object>> {

    private long count;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

}
