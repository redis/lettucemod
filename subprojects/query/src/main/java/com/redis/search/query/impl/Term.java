package com.redis.search.query.impl;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import com.redis.query.Query;

public class Term extends AbstractCondition {

    private final CharSequence delimiter;
    private final List<String> values;

    public Term(CharSequence delimiter, String... values) {
	Assert.notEmpty(values, "Must have at least one value");
	this.delimiter = delimiter;
	this.values = Arrays.asList(values);
    }

    @Override
    public String getQuery() {
	if (values.size() > 1 || Query.needsParens(values.get(0))) {
	    StringJoiner joiner = new StringJoiner(delimiter, Query.PAREN_OPEN, Query.PAREN_CLOSE);
	    values.forEach(joiner::add);
	    return joiner.toString();
	}
	return values.get(0);
    }
}
