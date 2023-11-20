package com.redis.search.query.filter;

public class NumericCondition implements Condition {

    private static final String FORMAT = "[%s %s]";

    private final NumericInterval interval;

    public NumericCondition(NumericInterval interval) {
	this.interval = interval;
    }

    @Override
    public String getQuery() {
	return String.format(FORMAT, interval.getLower(), interval.getUpper());
    }

}
