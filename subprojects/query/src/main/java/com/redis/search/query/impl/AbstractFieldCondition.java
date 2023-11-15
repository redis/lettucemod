package com.redis.search.query.impl;

import java.text.MessageFormat;

import com.redis.query.Field;

abstract class AbstractFieldCondition extends AbstractCondition {

    private static final String FORMAT = "@{0}:{1}";

    private final Field field;

    protected AbstractFieldCondition(Field field) {
	Assert.notNull(field, "Field must not be null");
	this.field = field;
    }

    @Override
    public String getQuery() {
	return MessageFormat.format(FORMAT, field.getName(), valueString());
    }

    protected abstract Object valueString();

}