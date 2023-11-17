package com.redis.search.query.impl;

import java.util.Arrays;
import java.util.Collection;

import com.redis.query.Condition;
import com.redis.query.Query;

/**
 * The intersection node evaluates to true if any of its children are true.
 *
 * In RS: {@code @f1:v1 @f2:v2}
 */
public class And extends CompositeCondition {

    public And(Collection<Condition> children) {
	super(Query.AND, children);
    }

    @Override
    public Condition and(Condition condition, Condition... conditions) {
	children.add(condition);
	children.addAll(Arrays.asList(conditions));
	return this;
    }

}
