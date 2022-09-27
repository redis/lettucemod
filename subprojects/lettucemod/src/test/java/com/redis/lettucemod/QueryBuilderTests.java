package com.redis.lettucemod;

import static com.redis.lettucemod.search.querybuilder.QueryBuilder.disjunct;
import static com.redis.lettucemod.search.querybuilder.QueryBuilder.intersect;
import static com.redis.lettucemod.search.querybuilder.QueryBuilder.optional;
import static com.redis.lettucemod.search.querybuilder.QueryBuilder.union;
import static com.redis.lettucemod.search.querybuilder.Values.between;
import static com.redis.lettucemod.search.querybuilder.Values.eq;
import static com.redis.lettucemod.search.querybuilder.Values.ge;
import static com.redis.lettucemod.search.querybuilder.Values.gt;
import static com.redis.lettucemod.search.querybuilder.Values.le;
import static com.redis.lettucemod.search.querybuilder.Values.lt;
import static com.redis.lettucemod.search.querybuilder.Values.tags;
import static com.redis.lettucemod.search.querybuilder.Values.value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.redis.lettucemod.search.querybuilder.GeoValue;
import com.redis.lettucemod.search.querybuilder.Node;
import com.redis.lettucemod.search.querybuilder.Value;
import com.redis.lettucemod.search.querybuilder.Values;

/**
 * Created by mnunberg on 2/23/18.
 */
class QueryBuilderTests {

	@Test
	void testTag() {
		Value v = tags("foo");
		assertEquals("{foo}", v.toString());
		v = tags("foo", "bar");
		assertEquals("{foo | bar}", v.toString());
	}

	@Test
	void testEmptyTag() {
		assertThrows(IllegalArgumentException.class, () -> tags());
	}

	@Test
	void testRange() {
		Value v = between(1, 10);
		assertEquals("[1 10]", v.toString());
		v = between(1, 10).inclusiveMax(false);
		assertEquals("[1 (10]", v.toString());
		v = between(1, 10).inclusiveMin(false);
		assertEquals("[(1 10]", v.toString());

		v = between(1.0, 10.1);
		assertEquals("[1.0 10.1]", v.toString());
		v = between(-1.0, 10.1).inclusiveMax(false);
		assertEquals("[-1.0 (10.1]", v.toString());
		v = between(-1.1, 150.61).inclusiveMin(false);
		assertEquals("[(-1.1 150.61]", v.toString());

		// le, gt, etc.
		// le, gt, etc.
		assertEquals("[42 42]", eq(42).toString());
		assertEquals("[-inf (42]", lt(42).toString());
		assertEquals("[-inf 42]", le(42).toString());
		assertEquals("[(-42 inf]", gt(-42).toString());
		assertEquals("[42 inf]", ge(42).toString());

		assertEquals("[42.0 42.0]", eq(42.0).toString());
		assertEquals("[-inf (42.0]", lt(42.0).toString());
		assertEquals("[-inf 42.0]", le(42.0).toString());
		assertEquals("[(42.0 inf]", gt(42.0).toString());
		assertEquals("[42.0 inf]", ge(42.0).toString());

		assertEquals("[(1587058030 inf]", gt(1587058030).toString());

		// string value
		assertEquals("s", value("s").toString());

		// Geo value
		assertEquals("[1.0 2.0 3.0 km]", new GeoValue(1.0, 2.0, 3.0, GeoValue.Unit.KILOMETERS).toString());
	}

	@Test
	void testIntersectionBasic() {
		Node n = intersect().add("name", "mark");
		assertEquals("@name:mark", n.toString());

		n = intersect().add("name", "mark", "dvir");
		assertEquals("@name:(mark dvir)", n.toString());

		n = intersect().add("name", Arrays.asList(Values.value("mark"), Values.value("shay")));
		assertEquals("@name:(mark shay)", n.toString());

		n = intersect("name", "meir");
		assertEquals("@name:meir", n.toString());

		n = intersect("name", Values.value("meir"), Values.value("rafi"));
		assertEquals("@name:(meir rafi)", n.toString());
	}

	@Test
	void testIntersectionNested() {
		Node n = intersect().add(union("name", value("mark"), value("dvir"))).add("time", between(100, 200))
				.add(disjunct("created", lt(1000)));
		assertEquals("(@name:(mark|dvir) @time:[100 200] -@created:[-inf (1000])", n.toString());
	}

	@Test
	void testOptional() {
		Node n = optional("name", tags("foo", "bar"));
		assertEquals("~@name:{foo | bar}", n.toString());

		n = optional(n, n);
		assertEquals("~(~@name:{foo | bar} ~@name:{foo | bar})", n.toString());
	}

}