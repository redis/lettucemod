package com.redis.search.query.filter;

public interface Condition {

    String getQuery();

    default Condition and(Condition condition) {
	return new And(this, condition);
    }

    default Condition or(Condition condition) {
	return new Or(this, condition);
    }

    default Condition not() {
	return new Not(this);
    }

    default Condition optional() {
	return new Optional(this);
    }

}
