package com.redis.search.query.filter;

public class Or extends CompositeCondition {

    public static final String DELIMITER = "|";

    public Or(Condition left, Condition right) {
	super(DELIMITER, left, right);
    }

}
