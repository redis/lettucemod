package com.redis.search.query.filter;

public class NumericBoundary {

    private static final String INFINITY = "inf";
    private static final String MINUS_INFINITY = "-inf";
    public static final NumericBoundary POSITIVE_INFINITY = new NumericBoundary(Double.POSITIVE_INFINITY, true);
    public static final NumericBoundary NEGATIVE_INFINITY = new NumericBoundary(Double.NEGATIVE_INFINITY, true);

    private static final String EXCLUSIVE_FORMAT = "(%s";

    private final Number value;
    private final boolean exclusive;

    public NumericBoundary(Number value, boolean exclusive) {
	this.value = value;
	this.exclusive = exclusive;
    }

    public String toString() {
	if (this == NEGATIVE_INFINITY) {
	    return MINUS_INFINITY;
	}
	if (this == POSITIVE_INFINITY) {
	    return INFINITY;
	}
	if (exclusive) {
	    return String.format(EXCLUSIVE_FORMAT, value);
	}
	return value.toString();
    }

    public static NumericBoundary inclusive(Number value) {
	return new NumericBoundary(value, false);
    }

    public static NumericBoundary exclusive(Number value) {
	return new NumericBoundary(value, true);
    }

}
