package com.redis.search.query.filter;

/**
 * The intersection node evaluates to true if any of its children are true.
 *
 * In RS: {@code @f1:v1 @f2:v2}
 */
public class And extends CompositeCondition {

    public static final String DELIMITER = " ";

    public And(Condition left, Condition right) {
	super(DELIMITER, left, right);
    }

}
