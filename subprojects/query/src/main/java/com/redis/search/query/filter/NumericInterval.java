package com.redis.search.query.filter;

public class NumericInterval {

    private final NumericBoundary lower;
    private final NumericBoundary upper;

    public NumericInterval(NumericBoundary lower, NumericBoundary upper) {
	this.lower = lower;
	this.upper = upper;
    }

    public NumericBoundary getLower() {
	return lower;
    }

    public NumericBoundary getUpper() {
	return upper;
    }

}
