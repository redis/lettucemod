package com.redis.search.query.filter;

public class Optional extends UnaryCondition {

    private static final String OPERATOR = "~";

    public Optional(Condition condition) {
	super(OPERATOR, condition);
    }

}
