package com.redis.query;

import com.redis.search.query.impl.And;
import com.redis.search.query.impl.CompositeCondition;
import com.redis.search.query.impl.GeoField;
import com.redis.search.query.impl.NumericField;
import com.redis.search.query.impl.Or;
import com.redis.search.query.impl.TagField;
import com.redis.search.query.impl.Term;
import com.redis.search.query.impl.TextField;
import com.redis.search.query.impl.UnaryOperatorCondition;

public abstract class Query {

    public static final String OR = "|";
    public static final String AND = " ";
    public static final String OPTIONAL = "~";
    public static final String NOT = "-";
    public static final String PAREN_OPEN = "(";
    public static final String PAREN_CLOSE = ")";
    public static final String NEGATIVE_INFINITY = "-inf";
    public static final String POSITIVE_INFINITY = "inf";
    public static final String TAG_OPEN = "{";
    public static final String TAG_CLOSE = "}";
    public static final String WILDCARD = "*";

    private Query() {
    }

    /**
     * Create a new intersection node with child nodes. An intersection node is true
     * if all its children are also true
     * 
     * @param conditions sub-condition to add
     * @return The node
     */
    public static CompositeCondition and(Condition... conditions) {
	return new And(conditions);
    }

    /**
     * Create a union node. Union nodes evaluate to true if <i>any</i> of its
     * children are true
     * 
     * @param conditions Child node
     * @return The union node
     */
    public static Condition or(Condition... conditions) {
	return new Or(conditions);
    }

    /**
     * Create a disjunct union node. This node evaluates to true if <b>all</b> of
     * its children are not true. Conversely, this node evaluates as false if
     * <b>any</b> of its children are true.
     * 
     * @param conditions
     * @return The node
     */
    public static Condition notOr(Condition... conditions) {
	return new UnaryOperatorCondition(NOT, new Or(conditions));
    }

    public static Condition not(Condition condition) {
	return new UnaryOperatorCondition(NOT, condition);
    }

    public static Condition optional(Condition condition) {
	return new UnaryOperatorCondition(OPTIONAL, condition);
    }

    public static TagField tag(String field) {
	return new TagField(field);
    }

    public static TextField text(String field) {
	return new TextField(field);
    }

    public static NumericField numeric(String field) {
	return new NumericField(field);
    }

    public static GeoField geo(String field) {
	return new GeoField(field);
    }

    public static Condition term(String term) {
	return new Term(AND, term);
    }

    public static Condition and(String... terms) {
	return new Term(AND, terms);
    }

    public static Condition or(String... terms) {
	return new Term(OR, terms);
    }

    public static Condition wildcard() {
	return term(WILDCARD);
    }

    public static boolean needsParens(String string) {
	return string.contains(" ") && !(string.startsWith("\"") && string.endsWith("\""));
    }

    public static String escapeTag(String value) {
	return value.replaceAll("([^a-zA-Z0-9\\*])", "\\\\$1");
    }

}