package com.redis.search.query.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.redis.query.Condition;

public class CompositeCondition extends AbstractCondition {

    private final CharSequence delimiter;

    private final List<Condition> children;

    public CompositeCondition(CharSequence delimiter, Condition... children) {
	this(delimiter, Arrays.asList(children));
    }

    public CompositeCondition(CharSequence delimiter, Collection<Condition> children) {
	this.delimiter = delimiter;
	this.children = new ArrayList<>(children);
    }

    protected boolean shouldUseParens() {
	return children.size() > 1;
    }

    @Override
    public String getQuery() {
	return String.join(delimiter, children.stream().map(Condition::getQuery).toArray(String[]::new));
    }

}