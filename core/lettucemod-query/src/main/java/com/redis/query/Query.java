package com.redis.query;

import com.redis.search.query.filter.GeoField;
import com.redis.search.query.filter.NumericField;
import com.redis.search.query.filter.TagField;
import com.redis.search.query.filter.TermCondition;
import com.redis.search.query.filter.TextField;
import com.redis.search.query.filter.VectorField;
import com.redis.search.query.filter.Wildcard;

public abstract class Query {

    public static final Wildcard WILDCARD = new Wildcard();

    private Query() {
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

    public static VectorField vector(String field) {
        return new VectorField(field);
    }

    public static TermCondition term(String term) {
        return new TermCondition(term);
    }

    public static Wildcard wildcard() {
        return WILDCARD;
    }

    public static String escapeTag(String value) {
        return value.replaceAll("([^a-zA-Z0-9\\*])", "\\\\$1");
    }

}
