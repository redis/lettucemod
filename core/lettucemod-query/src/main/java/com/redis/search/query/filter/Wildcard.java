package com.redis.search.query.filter;

public class Wildcard implements Condition {

    private static final String ASTERISK = "*";

    @Override
    public String getQuery() {
	return ASTERISK;
    }

    @Override
    public Condition and(Condition condition) {
	return condition;
    }

    @Override
    public Condition or(Condition condition) {
	return condition;
    }

    @Override
    public Condition not() {
	return this;
    }

    @Override
    public Condition optional() {
	return this;
    }

}
