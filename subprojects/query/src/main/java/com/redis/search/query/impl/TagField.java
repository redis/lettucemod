package com.redis.search.query.impl;

import com.redis.query.Query;

public class TagField extends AbstractField {

    public TagField(String name) {
	super(name);
    }

    public TagFieldCondition in(String... values) {
	return new TagFieldCondition(this, Query.OR, values);
    }

    public TagFieldCondition eq(String value) {
	return in(value);
    }

}
