package com.redis.search.query.filter;

public class UnaryCondition implements Condition {

    private final CharSequence operator;
    private final Condition condition;

    protected UnaryCondition(CharSequence operator, Condition condition) {
	this.operator = operator;
	this.condition = condition;
    }

    @Override
    public String getQuery() {
	StringBuilder builder = new StringBuilder();
	builder.append(operator);
	builder.append(operandString());
	return builder.toString();
    }

    private String operandString() {
	if (condition instanceof FieldCondition) {
	    return condition.getQuery();
	}
	return Utils.parens(condition.getQuery());
    }

}
