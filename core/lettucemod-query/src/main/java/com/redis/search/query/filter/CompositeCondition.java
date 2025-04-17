package com.redis.search.query.filter;

public class CompositeCondition implements Condition {

    private final Condition left;
    private final Condition right;
    private final CharSequence delimiter;

    public CompositeCondition(CharSequence delimiter, Condition left, Condition right) {
	this.delimiter = delimiter;
	this.left = left;
	this.right = right;
    }

    @Override
    public String getQuery() {
	return String.join(delimiter, left.getQuery(), right.getQuery());
    }

}