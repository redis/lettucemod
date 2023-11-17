package com.redis.search.query.impl;

import com.redis.query.Condition;
import com.redis.query.Query;

public abstract class AbstractCondition implements Condition {

    @Override
    public Condition and(Condition condition, Condition... conditions) {
	return Query.and(Query.list(this, condition, conditions));
    }

    @Override
    public Condition or(Condition condition, Condition... conditions) {
	return Query.or(Query.list(this, condition, conditions));
    }

    @Override
    public Condition optional() {
	return Query.optional(this);
    }

    @Override
    public Condition not() {
	return Query.not(this);
    }

}
