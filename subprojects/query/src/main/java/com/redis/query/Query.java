package com.redis.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.redis.search.query.impl.And;
import com.redis.search.query.impl.CompositeCondition;
import com.redis.search.query.impl.GeoField;
import com.redis.search.query.impl.NumericField;
import com.redis.search.query.impl.Or;
import com.redis.search.query.impl.TagField;
import com.redis.search.query.impl.Term;
import com.redis.search.query.impl.TextField;
import com.redis.search.query.impl.UnaryOperatorCondition;
import com.redis.search.query.impl.VectorRangeField;

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

    private static <T> List<T> list(T condition, T[] conditions) {
	List<T> all = new ArrayList<>();
	all.add(condition);
	all.addAll(Arrays.asList(conditions));
	return all;
    }

    public static <T> List<T> list(T first, T second, T[] conditions) {
	List<T> all = new ArrayList<>();
	all.add(first);
	all.add(second);
	all.addAll(Arrays.asList(conditions));
	return all;
    }

    /**
     * Create a new intersection node with child nodes. An intersection node is true
     * if all its children are also true
     * 
     * @param conditions sub-condition to add
     * @return The node
     */
    public static CompositeCondition and(Condition condition, Condition... conditions) {
	return and(list(condition, conditions));
    }

    public static CompositeCondition and(Collection<Condition> conditions) {
	return new And(conditions);
    }

    /**
     * Create a union node. Union nodes evaluate to true if <i>any</i> of its
     * children are true
     * 
     * @param conditions Child node
     * @return The union node
     */
    public static Condition or(Condition condition, Condition... conditions) {
	return or(list(condition, conditions));
    }

    public static Condition or(Collection<Condition> conditions) {
	return new Or(conditions);
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

    public static VectorRangeField vectorRange(String field) {
	return new VectorRangeField(field);
    }

    public static Condition term(String term) {
	return new Term(AND, Arrays.asList(term));
    }

    public static Condition and(String term, String... terms) {
	return new Term(AND, list(term, terms));
    }

    public static Condition or(String term, String... terms) {
	return new Term(OR, list(term, terms));
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