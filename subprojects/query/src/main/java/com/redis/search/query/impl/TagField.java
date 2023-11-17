package com.redis.search.query.impl;

import com.redis.query.Query;

public class TagField extends AbstractField {

    public TagField(String name) {
	super(name);
    }

    public TagCondition in(String... values) {
	return new TagCondition(this, Query.OR, values);
    }

    public TagCondition eq(String value) {
	return in(value);
    }

}
