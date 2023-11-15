package com.redis.search.query.impl;

import com.redis.query.Condition;
import com.redis.query.Query;

public class UnaryOperatorCondition extends AbstractCondition {

    private final CharSequence operator;
    private final Condition condition;

    public UnaryOperatorCondition(CharSequence operator, Condition condition) {
	this.operator = operator;
	this.condition = condition;
    }

    @Override
    public String getQuery() {
	boolean useParens = !(condition instanceof AbstractFieldCondition);
	StringBuilder builder = new StringBuilder();
	builder.append(operator);
	if (useParens) {
	    builder.append(Query.PAREN_OPEN);
	}
	builder.append(condition.getQuery());
	if (useParens) {
	    builder.append(Query.PAREN_CLOSE);
	}
	return builder.toString();
    }

}
