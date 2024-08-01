package com.redis.search.query.filter;

public class TextCondition extends FieldCondition {

    private static final String VALUE_FORMAT = "(%s)";

    public TextCondition(Field field, TermCondition condition) {
	super(field, condition);
    }

    public TextCondition and(String term) {
	this.condition = this.condition.and(new TermCondition(term));
	return this;
    }

    public TextCondition or(String term) {
	this.condition = this.condition.or(new TermCondition(term));
	return this;
    }

    @Override
    protected String valueString() {
	if (condition instanceof TermCondition) {
	    TermCondition termCondition = (TermCondition) condition;
	    String query = termCondition.getQuery();
	    if (!query.contains(" ")) {
		return query;
	    }
	}
	return String.format(VALUE_FORMAT, condition.getQuery());
    }

}
