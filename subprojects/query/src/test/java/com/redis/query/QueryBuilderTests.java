package com.redis.query;

import static com.redis.query.Query.and;
import static com.redis.query.Query.geo;
import static com.redis.query.Query.numeric;
import static com.redis.query.Query.optional;
import static com.redis.query.Query.tag;
import static com.redis.query.Query.text;
import static com.redis.query.Query.or;
import static com.redis.query.Query.term;
import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import com.redis.search.query.impl.Distance;
import com.redis.search.query.impl.GeoCoordinates;
import com.redis.search.query.impl.NumericField;

/**
 * Created by mnunberg on 2/23/18.
 */
class QueryBuilderTests {

    @Test
    void testTag() {
	Condition tagEq = tag("myField").eq("foo");
	assertEquals("@myField:{foo}", tagEq.getQuery());
	Condition tagIn = tag("myField").in("foo", "bar");
	assertEquals("@myField:{foo|bar}", tagIn.getQuery());
    }

    @Test
    void testTagWithSpace() {
	Condition condition = tag("myField").eq("foo bar");
	assertEquals("@myField:{foo\\ bar}", condition.getQuery());
	condition = tag("myField").in("foo bar", "bar");
	assertEquals("@myField:{foo\\ bar|bar}", condition.getQuery());
    }

    @Test
    void testRange() {
	NumericField field = numeric("name");
	Condition condition = field.between(1, 10);
	assertEquals("@name:[1 10]", condition.getQuery());
	condition = field.between(1, 10).exclusiveTo(true);
	assertEquals("@name:[1 (10]", condition.getQuery());
	condition = field.between(1, 10).exclusiveFrom(true);
	assertEquals("@name:[(1 10]", condition.getQuery());

	condition = field.between(1.0, 10.1);
	assertEquals("@name:[1.0 10.1]", condition.getQuery());
	condition = field.between(-1.0, 10.1).exclusiveTo(true);
	assertEquals("@name:[-1.0 (10.1]", condition.getQuery());
	condition = field.between(-1.1, 150.61).exclusiveFrom(true);
	assertEquals("@name:[(-1.1 150.61]", condition.getQuery());

	// le, gt, etc.
	// le, gt, etc.
	assertEquals("@name:[42 42]", field.eq(42).getQuery());
	assertEquals("@name:[-inf (42]", field.lt(42).getQuery());
	assertEquals("@name:[-inf 42]", field.le(42).getQuery());
	assertEquals("@name:[(-42 inf]", field.gt(-42).getQuery());
	assertEquals("@name:[42 inf]", field.ge(42).getQuery());

	assertEquals("@name:[42.0 42.0]", field.eq(42.0).getQuery());
	assertEquals("@name:[-inf (42.0]", field.lt(42.0).getQuery());
	assertEquals("@name:[-inf 42.0]", field.le(42.0).getQuery());
	assertEquals("@name:[(42.0 inf]", field.gt(42.0).getQuery());
	assertEquals("@name:[42.0 inf]", field.ge(42.0).getQuery());

	assertEquals("@name:[(1587058030 inf]", field.gt(1587058030).getQuery());

    }

    @Test
    void testGeo() {
	// Geo value
	assertEquals("@name:[1 2 3 km]",
		geo("name").within(GeoCoordinates.lon(1).lat(2), Distance.kilometers(3)).getQuery());
    }

    @Test
    void testIntersectionBasic() {
	Condition condition = text("name").eq("mark");
	assertEquals("@name:mark", condition.getQuery());
	condition = text("name").and("mark", "dvir");
	assertEquals("@name:(mark dvir)", condition.getQuery());

    }

    @Test
    void testIntersectionNested() {
	Condition condition = and(text("name").or("mark", "dvir"), numeric("time").between(100, 200),
		numeric("created").lt(1000).not());
	assertEquals("@name:(mark|dvir) @time:[100 200] -@created:[-inf (1000]", condition.getQuery());
    }

    @Test
    void testOptional() {
	Condition condition = optional(tag("name").in("foo", "bar"));
	assertEquals("~@name:{foo|bar}", condition.getQuery());
	condition = optional(and(condition, condition));
	assertEquals("~(~@name:{foo|bar} ~@name:{foo|bar})", condition.getQuery());
    }

    @Test
    void testBasic() {
	Condition condition = term("hello").and(or("world", "foo")).and(term("\"bar baz\"")).and(term("bbbb"));
	assertEquals("hello (world|foo) \"bar baz\" bbbb", condition.getQuery());
    }

    @Test
    void testEscapeTag() {
	String tag = "a b c";
	assertEquals("a\\ b\\ c", Query.escapeTag(tag));
    }
    
    @Test
    void wildcard() {
	assertEquals("*", Query.wildcard().getQuery());
    }

}