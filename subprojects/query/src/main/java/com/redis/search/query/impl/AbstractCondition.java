package com.redis.search.query.impl;

import com.redis.query.Condition;
import com.redis.query.Query;

public abstract class AbstractCondition implements Condition {

    @Override
    public Condition and(Condition condition) {
	return Query.and(this, condition);
    }

    @Override
    public Condition or(Condition condition) {
	return Query.or(this, condition);
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
