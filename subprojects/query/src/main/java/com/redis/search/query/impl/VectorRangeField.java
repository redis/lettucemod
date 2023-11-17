package com.redis.search.query.impl;

public class VectorRangeField extends AbstractField {

    public VectorRangeField(String name) {
	super(name);
    }

    public VectorRangeCondition radius(Number radius, String paramName) {
	return new VectorRangeCondition(this, radius, paramName);
    }

}
