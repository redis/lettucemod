package com.redis.search.query.filter;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import com.redis.query.Query;

public class TagCondition implements Condition {

    private final List<String> values;

    public TagCondition(String... values) {
	Utils.notEmpty(values, "Must have at least one tag");
	this.values = Arrays.asList(values);
    }

    @Override
    public String getQuery() {
	StringJoiner joiner = new StringJoiner(Or.DELIMITER, "{", "}");
	values.stream().map(Query::escapeTag).forEach(joiner::add);
	return joiner.toString();
    }

}
