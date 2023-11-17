package com.redis.search.query.impl;

import java.util.Collection;

import com.redis.query.Condition;

public class CompositeCondition extends AbstractCondition {

    private final CharSequence delimiter;

    protected final Collection<Condition> children;

    public CompositeCondition(CharSequence delimiter, Collection<Condition> children) {
	this.delimiter = delimiter;
	this.children = children;
    }

    protected boolean shouldUseParens() {
	return children.size() > 1;
    }

    @Override
    public String getQuery() {
	return String.join(delimiter, children.stream().map(Condition::getQuery).toArray(String[]::new));
    }

}