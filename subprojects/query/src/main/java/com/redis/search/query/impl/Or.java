package com.redis.search.query.impl;

import java.util.Arrays;
import java.util.Collection;

import com.redis.query.Condition;
import com.redis.query.Query;

public class Or extends CompositeCondition {

    public Or(Collection<Condition> children) {
	super(Query.OR, children);
    }

    @Override
    public Condition or(Condition condition, Condition... conditions) {
	children.add(condition);
	children.addAll(Arrays.asList(conditions));
	return this;
    }

}
