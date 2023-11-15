package com.redis.search.query.impl;

import com.redis.query.Field;

public class TextFieldCondition extends AbstractFieldCondition {

    private final Term condition;

    public TextFieldCondition(Field field, Term condition) {
	super(field);
	this.condition = condition;
    }

    @Override
    protected String valueString() {
	return condition.getQuery();
    }

}
