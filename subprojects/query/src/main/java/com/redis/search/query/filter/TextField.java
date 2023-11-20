package com.redis.search.query.filter;

public class TextField extends AbstractField {

    public TextField(String name) {
	super(name);
    }

    public TextCondition term(String value) {
	return new TextCondition(this, new TermCondition(value));
    }

}
