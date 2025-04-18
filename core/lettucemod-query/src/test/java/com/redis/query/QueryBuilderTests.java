package com.redis.query;

import static com.redis.query.Query.geo;
import static com.redis.query.Query.numeric;
import static com.redis.query.Query.tag;
import static com.redis.query.Query.term;
import static com.redis.query.Query.text;
import static com.redis.query.Query.vector;
import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import com.redis.search.query.filter.Condition;
import com.redis.search.query.filter.Distance;
import com.redis.search.query.filter.GeoCoordinates;
import com.redis.search.query.filter.NumericBoundary;
import com.redis.search.query.filter.NumericField;
import com.redis.search.query.filter.VectorField;

/**
 * Created by mnunberg on 2/23/18.
 */
class QueryBuilderTests {

    @Test
    void testTag() {
        Condition tagEq = tag("myField").in("foo");
        assertEquals("@myField:{foo}", tagEq.getQuery());
        Condition tagIn = tag("myField").in("foo", "bar");
        assertEquals("@myField:{foo|bar}", tagIn.getQuery());
    }

    @Test
    void testTagWithSpace() {
        Condition condition = tag("myField").in("foo bar");
        assertEquals("@myField:{foo\\ bar}", condition.getQuery());
        condition = tag("myField").in("foo bar", "bar");
        assertEquals("@myField:{foo\\ bar|bar}", condition.getQuery());
    }

    @Test
    void testNumeric() {
        NumericField field = numeric("name");
        Condition condition = field.between(1, 10);
        assertEquals("@name:[1 10]", condition.getQuery());
        condition = field.between(1, NumericBoundary.exclusive(10));
        assertEquals("@name:[1 (10]", condition.getQuery());
        condition = field.between(NumericBoundary.exclusive(1), 10);
        assertEquals("@name:[(1 10]", condition.getQuery());

        condition = field.between(1.0, 10.1);
        assertEquals("@name:[1.0 10.1]", condition.getQuery());
        condition = field.between(-1.0, NumericBoundary.exclusive(10.1));
        assertEquals("@name:[-1.0 (10.1]", condition.getQuery());
        condition = field.between(NumericBoundary.exclusive(-1.1), 150.61);
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
    void testVectorRange() {
        VectorField field = vector("name");
        Condition condition = field.range(1.23, "myvec");
        assertEquals("@name:[VECTOR_RANGE 1.23 $myvec]", condition.getQuery());
    }

    @Test
    void testVectorKNN() {
        VectorField field = vector("name");
        assertEquals("*=>[KNN 10 @name $myvec]", field.knn(10, "myvec").getQuery());
        assertEquals("*=>[KNN $mynum @name $myvec]", field.knn("mynum", "myvec").getQuery());
        assertEquals("@published_year:[2020 2022]=>[KNN 10 @vector_field $query_vec]",
                vector("vector_field").knn(10, "query_vec").and(numeric("published_year").between(2020, 2022)).getQuery());
    }

    @Test
    void testGeo() {
        // Geo value
        assertEquals("@name:[1 2 3 km]", geo("name").within(GeoCoordinates.lon(1).lat(2), Distance.kilometers(3)).getQuery());
    }

    @Test
    void testIntersectionBasic() {
        Condition condition = text("name").term("mark");
        assertEquals("@name:mark", condition.getQuery());
        condition = text("name").term("mark").and("dvir");
        assertEquals("@name:(mark dvir)", condition.getQuery());

    }

    @Test
    void testIntersectionNested() {
        Condition condition = text("name").term("mark").or("dvir").and(numeric("time").between(100, 200))
                .and(numeric("created").lt(1000).not());
        assertEquals("@name:(mark|dvir) @time:[100 200] -@created:[-inf (1000]", condition.getQuery());
    }

    @Test
    void testOptional() {
        Condition condition = tag("name").in("foo", "bar").optional();
        assertEquals("~@name:{foo|bar}", condition.getQuery());
        condition = condition.and(condition).optional();
        assertEquals("~(~@name:{foo|bar} ~@name:{foo|bar})", condition.getQuery());
    }

    @Test
    void testBasic() {
        Condition condition = term("hello").and(term("world").or(term("foo"))).and(term("\"bar baz\"")).and(term("bbbb"));
        assertEquals("hello world|foo \"bar baz\" bbbb", condition.getQuery());
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
