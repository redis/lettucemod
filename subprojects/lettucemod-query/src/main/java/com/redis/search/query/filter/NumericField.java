package com.redis.search.query.filter;

import static com.redis.search.query.filter.NumericBoundary.*;

public class NumericField extends AbstractField {

    public NumericField(String name) {
	super(name);
    }

    private NumericInterval to(Number value) {
	return to(inclusive(value));
    }

    private NumericInterval toExclusive(Number value) {
	return to(exclusive(value));
    }

    private NumericInterval to(NumericBoundary upper) {
	return new NumericInterval(NEGATIVE_INFINITY, upper);
    }

    private NumericInterval from(Number value) {
	return from(inclusive(value));
    }

    private NumericInterval fromExclusive(Number value) {
	return from(exclusive(value));
    }

    private NumericInterval from(NumericBoundary lower) {
	return new NumericInterval(lower, POSITIVE_INFINITY);
    }

    private FieldCondition condition(NumericInterval interval) {
	return new FieldCondition(this, new NumericCondition(interval));
    }

    public FieldCondition between(Number from, Number to) {
	return between(inclusive(from), inclusive(to));
    }

    public FieldCondition between(NumericBoundary lower, Number upper) {
	return between(lower, inclusive(upper));
    }

    public FieldCondition between(Number lower, NumericBoundary upper) {
	return between(inclusive(lower), upper);
    }

    public FieldCondition betweenExclusive(Number from, Number to) {
	return between(exclusive(from), exclusive(to));
    }

    public FieldCondition between(NumericBoundary lower, NumericBoundary upper) {
	return condition(new NumericInterval(lower, upper));
    }

    public FieldCondition eq(Number value) {
	return between(value, value);
    }

    public FieldCondition le(Number value) {
	return condition(to(value));
    }

    public FieldCondition lt(Number value) {
	return condition(toExclusive(value));
    }

    public FieldCondition ge(Number value) {
	return condition(from(value));
    }

    public FieldCondition gt(Number value) {
	return condition(fromExclusive(value));
    }

}
