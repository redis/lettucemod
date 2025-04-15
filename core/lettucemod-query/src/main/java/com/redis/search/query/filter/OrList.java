package com.redis.search.query.filter;

import jdk.internal.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OrList implements Condition {
    public static final String DELIMITER = "|";
    private final List<Condition> conditions;

    public OrList(Condition...conditions) {
        this.conditions= Arrays.asList(conditions);
    }

    public String getQuery() {
        if (conditions == null || conditions.isEmpty()) {
            return "";
        }
        // TODO StringJoiner
        String joinedOrCondition= conditions.stream()
                .map(c ->"("+ (c.getQuery() == null ? "" : c.getQuery())+")")
                .collect(Collectors.joining(DELIMITER));
        return "("+joinedOrCondition+")";
    }

}
