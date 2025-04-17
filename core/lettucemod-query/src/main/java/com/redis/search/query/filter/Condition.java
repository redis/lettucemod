package com.redis.search.query.filter;

import java.util.List;

public interface Condition {

    String getQuery();

    default Condition and(Condition condition) {
	return new And(this, condition);
    }

    default Condition or(Condition condition) {
	return new Or(this, condition);
    }

    public static Condition orList(final Condition...conditions) {
        return new OrList(conditions);
    }

    default Condition not() {
	return new Not(this);
    }

    default Condition optional() {
	return new Optional(this);
    }

}
