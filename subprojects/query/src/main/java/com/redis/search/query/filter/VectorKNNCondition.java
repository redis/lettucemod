package com.redis.search.query.filter;

import com.redis.query.Query;

public class VectorKNNCondition implements Condition {

    /**
     * The basic syntax is "*=>[ KNN {num|$num} @vector $query_vec ]"
     */
    private static final String FORMAT = "%s=>[KNN %s @%s $%s]";

    private final Field field;
    private int num;
    private String numParam;
    private final String vectorParam;
    private Condition condition = Query.wildcard();

    public VectorKNNCondition(Field field, int num, String vectorParam) {
	this.field = field;
	this.num = num;
	this.vectorParam = vectorParam;
    }

    public VectorKNNCondition(Field field, String numParam, String vectorParam) {
	this.field = field;
	this.numParam = numParam;
	this.vectorParam = vectorParam;
    }

    @Override
    public String getQuery() {
	return String.format(FORMAT, conditionString(), numString(), field.getName(), vectorParam);
    }

    private String conditionString() {
	if (condition == Query.WILDCARD) {
	    return condition.getQuery();
	}
	if (condition instanceof FieldCondition) {
	    return condition.getQuery();
	}
	return Utils.parens(condition.getQuery());
    }

    private String numString() {
	if (numParam == null) {
	    return String.valueOf(num);
	}
	return "$" + numParam;
    }

    private VectorKNNCondition condition(Condition condition) {
	this.condition = condition;
	return this;
    }

    @Override
    public VectorKNNCondition and(Condition condition) {
	return condition(this.condition.and(condition));
    }

    @Override
    public VectorKNNCondition or(Condition condition) {
	return condition(this.condition.or(condition));
    }

    @Override
    public VectorKNNCondition not() {
	return condition(this.condition.not());
    }

    @Override
    public VectorKNNCondition optional() {
	return condition(this.condition.optional());
    }

}
