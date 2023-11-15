package com.redis.search.query.impl;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import com.redis.query.Field;
import com.redis.query.Query;

public class TagFieldCondition extends AbstractFieldCondition {

    private final List<String> values;
    private final CharSequence delimiter;

    public TagFieldCondition(Field field, CharSequence delimiter, String... values) {
	super(field);
	Assert.notEmpty(values, "Must have at least one tag");
	this.delimiter = delimiter;
	this.values = Arrays.asList(values);
    }

    @Override
    protected Object valueString() {
	StringJoiner joiner = new StringJoiner(delimiter, Query.TAG_OPEN, Query.TAG_CLOSE);
	values.stream().map(Query::escapeTag).forEach(joiner::add);
	return joiner.toString();
    }

}
