package com.redis.search.query.filter;

public class FieldCondition implements Condition {

    private static final String FORMAT = "@%s:%s";

    protected final Field field;
    protected Condition condition;

    public FieldCondition(Field field, Condition condition) {
	Utils.notNull(field, "Field must not be null");
	Utils.notNull(condition, "Condition must not be null");
	this.field = field;
	this.condition = condition;
    }

    @Override
    public String getQuery() {
	return String.format(FORMAT, field.getName(), valueString());
    }

    protected String valueString() {
	return condition.getQuery();
    }

}