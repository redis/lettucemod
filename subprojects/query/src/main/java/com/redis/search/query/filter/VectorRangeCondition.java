package com.redis.search.query.filter;

public class VectorRangeCondition implements Condition {

    private static final String FORMAT = "[VECTOR_RANGE %s $%s]";

    private final Number radius;
    private final String vector;

    public VectorRangeCondition(Number radius, String vector) {
	this.radius = radius;
	this.vector = vector;
    }

    @Override
    public String getQuery() {
	return String.format(FORMAT, radius, vector);
    }
}
