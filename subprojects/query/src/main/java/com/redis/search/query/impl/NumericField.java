package com.redis.search.query.impl;

public class NumericField extends AbstractField {

    public static final Number POSITIVE_INFINITY = Double.POSITIVE_INFINITY;
    public static final Number NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    public NumericField(String name) {
	super(name);
    }

    public NumericCondition between(Number from, Number to) {
	return new NumericCondition(this, from, to);
    }

    public NumericCondition eq(Number value) {
	return new NumericCondition(this, value, value);
    }

    public NumericCondition le(Number value) {
	return new NumericCondition(this, NEGATIVE_INFINITY, value);
    }

    public NumericCondition lt(Number value) {
	return le(value).exclusiveTo(true);
    }

    public NumericCondition ge(Number value) {
	return new NumericCondition(this, value, POSITIVE_INFINITY);
    }

    public NumericCondition gt(Number value) {
	return ge(value).exclusiveFrom(true);
    }

}
