package com.redis.search.query.impl;

import java.util.Arrays;

import com.redis.query.Query;

public class TextField extends AbstractField {

    public TextField(String name) {
	super(name);
    }

    public TextCondition eq(String value) {
	return and(value);
    }

    public TextCondition or(String... values) {
	return new TextCondition(this, new Term(Query.OR, Arrays.asList(values)));
    }

    public TextCondition and(String... values) {
	return new TextCondition(this, new Term(Query.AND, Arrays.asList(values)));
    }
}
