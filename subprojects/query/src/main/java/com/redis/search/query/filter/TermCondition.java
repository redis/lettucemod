package com.redis.search.query.filter;

public class TermCondition implements Condition {

    private final String value;

    public TermCondition(String value) {
	this.value = value;
    }

    @Override
    public String getQuery() {
	return value;
    }

}
