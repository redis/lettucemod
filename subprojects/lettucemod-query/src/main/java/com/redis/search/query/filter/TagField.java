package com.redis.search.query.filter;

public class TagField extends AbstractField {

    public TagField(String name) {
	super(name);
    }

    public FieldCondition in(String... values) {
	return new FieldCondition(this, new TagCondition(values));
    }

}
