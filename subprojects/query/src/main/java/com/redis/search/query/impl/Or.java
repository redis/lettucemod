package com.redis.search.query.impl;

import com.redis.query.Condition;
import com.redis.query.Query;

public class Or extends CompositeCondition {

    public Or(Condition... children) {
	super(Query.OR, children);
    }

}
