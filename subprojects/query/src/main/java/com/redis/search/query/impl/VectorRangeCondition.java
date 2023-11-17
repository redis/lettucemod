package com.redis.search.query.impl;

import java.text.MessageFormat;

public class VectorRangeCondition extends AbstractFieldCondition {

    private static final String FORMAT = "[VECTOR_RANGE {0} ${1}]";

    private final Number radius;
    private final String paramName;

    public VectorRangeCondition(VectorRangeField field, Number radius, String paramName) {
	super(field);
	this.radius = radius;
	this.paramName = paramName;
    }

    @Override
    protected String valueString() {
	return MessageFormat.format(FORMAT, radius, paramName);
    }
}
