package com.redis.search.query.impl;

import com.redis.query.Query;

public class TextField extends AbstractField {

    public TextField(String name) {
	super(name);
    }

    public TextFieldCondition eq(String value) {
	return and(value);
    }

    public TextFieldCondition or(String... values) {
	return new TextFieldCondition(this, new Term(Query.OR, values));
    }

    public TextFieldCondition and(String... values) {
	return new TextFieldCondition(this, new Term(Query.AND, values));
    }
}
