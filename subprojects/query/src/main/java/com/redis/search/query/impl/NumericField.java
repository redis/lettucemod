package com.redis.search.query.impl;

public class NumericField extends AbstractField {

    public static final Number POSITIVE_INFINITY = Double.POSITIVE_INFINITY;
    public static final Number NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    public NumericField(String name) {
	super(name);
    }

    public NumericFieldCondition between(Number from, Number to) {
	return new NumericFieldCondition(this, from, to);
    }

    public NumericFieldCondition eq(Number value) {
	return new NumericFieldCondition(this, value, value);
    }

    public NumericFieldCondition le(Number value) {
	return new NumericFieldCondition(this, NEGATIVE_INFINITY, value);
    }

    public NumericFieldCondition lt(Number value) {
	return le(value).exclusiveTo(true);
    }

    public NumericFieldCondition ge(Number value) {
	return new NumericFieldCondition(this, value, POSITIVE_INFINITY);
    }

    public NumericFieldCondition gt(Number value) {
	return ge(value).exclusiveFrom(true);
    }

}
