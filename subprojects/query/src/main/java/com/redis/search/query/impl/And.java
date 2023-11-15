package com.redis.search.query.impl;

import com.redis.query.Condition;
import com.redis.query.Query;

/**
 * The intersection node evaluates to true if any of its children are true.
 *
 * In RS: {@code @f1:v1 @f2:v2}
 */
public class And extends CompositeCondition {

    public And(Condition... children) {
	super(Query.AND, children);
    }

}
