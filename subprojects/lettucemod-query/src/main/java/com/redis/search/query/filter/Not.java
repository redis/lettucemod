package com.redis.search.query.filter;

public class Not extends UnaryCondition {

    private static final String OPERATOR = "-";

    public Not(Condition condition) {
	super(OPERATOR, condition);
    }

}
