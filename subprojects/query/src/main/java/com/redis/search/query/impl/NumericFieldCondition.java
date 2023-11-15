package com.redis.search.query.impl;

import java.text.MessageFormat;

import com.redis.query.Query;

public class NumericFieldCondition extends AbstractFieldCondition {

    private static final String FORMAT = "[{0} {1}]";

    private final Number from;
    private final Number to;
    private boolean exclusiveFrom;
    private boolean exclusiveTo;

    public NumericFieldCondition(NumericField field, Number from, Number to) {
	super(field);
	this.from = from;
	this.to = to;
    }

    public NumericFieldCondition exclusiveFrom(boolean exclusive) {
	this.exclusiveFrom = exclusive;
	return this;
    }

    public NumericFieldCondition exclusiveTo(boolean exclusive) {
	this.exclusiveTo = exclusive;
	return this;
    }

    private CharSequence value(Number number, boolean exclusive) {
	if (isNegativeInfinity(number)) {
	    return Query.NEGATIVE_INFINITY;
	}
	if (isPositiveInfinity(number)) {
	    return Query.POSITIVE_INFINITY;
	}
	String value = number.toString();
	if (exclusive) {
	    return Query.PAREN_OPEN + value;
	}
	return value;
    }

    private boolean isPositiveInfinity(Number number) {
	return number == NumericField.POSITIVE_INFINITY;
    }

    private boolean isNegativeInfinity(Number number) {
	return number == NumericField.NEGATIVE_INFINITY;
    }

    @Override
    protected String valueString() {
	return MessageFormat.format(FORMAT, value(from, exclusiveFrom), value(to, exclusiveTo));
    }

}
