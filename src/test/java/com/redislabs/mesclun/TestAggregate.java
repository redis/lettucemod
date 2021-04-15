package com.redislabs.mesclun;

import com.redislabs.mesclun.search.AggregateOptions;
import com.redislabs.mesclun.search.AggregateResults;
import com.redislabs.mesclun.search.AggregateWithCursorResults;
import com.redislabs.mesclun.search.Cursor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("unchecked")
public class TestAggregate extends AbstractSearchTest {

    private static List<Map<String, String>> beers;

    @BeforeAll
    public static void initializeIndex() throws IOException {
        beers = createBeerIndex();
    }

    @Test
    public void testLoad() {
        AggregateResults<String> results = sync.aggregate(INDEX, "*", AggregateOptions.<String, String>builder().load(ID).load(NAME).load(STYLE).build());
        Assertions.assertEquals(1, results.getCount());
        assertEquals(BEER_COUNT, results.size());
        Map<String, Map<String, String>> beerMap = beers.stream().collect(Collectors.toMap(b -> b.get(ID), b -> b));
        for (Map<String, Object> result : results) {
            String id = (String) result.get(ID);
            Map<String, String> beer = beerMap.get(id);
            assertEquals(beer.get(NAME).toLowerCase(), ((String) result.get(NAME)).toLowerCase());
            String style = beer.get(STYLE);
            if (style != null) {
                assertEquals(style.toLowerCase(), ((String) result.get(STYLE)).toLowerCase());
            }
        }
    }

    @Test
    public void group() {
        AggregateResults<String> results = sync.aggregate(INDEX, "*", AggregateOptions.<String, String>builder().groupBy(Collections.singletonList(STYLE), AggregateOptions.Operation.GroupBy.Reducer.Avg.<String, String>builder().property(ABV).as(ABV).build()).sortBy(AggregateOptions.Operation.SortBy.Property.<String, String>builder().property(ABV).order(AggregateOptions.Operation.Order.Desc).build()).limit(0, 20).build());
        assertEquals(100, results.getCount());
        List<Double> abvs = results.stream().map(r -> Double.parseDouble((String) r.get(ABV))).collect(Collectors.toList());
        assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
        assertEquals(20, results.size());
    }

    @Test
    public void groupToList() {
        AggregateResults<String> results = sync.aggregate(INDEX, "*", AggregateOptions.<String, String>builder().groupBy(Collections.singletonList(STYLE), AggregateOptions.Operation.GroupBy.Reducer.ToList.<String, String>builder().property(NAME).as("names").build(), AggregateOptions.Operation.GroupBy.Reducer.Count.of("count")).limit(0, 1).build());
        assertEquals(100, results.getCount());
        assertEquals("belgian ipa", ((String) results.get(0).get(STYLE)).toLowerCase());
        Object names = results.get(0).get("names");
        assertEquals(17, ((List<String>) names).size());
    }


    @Test
    public void cursor() {
        AggregateWithCursorResults<String> results = sync.aggregate(INDEX, "*", Cursor.builder().build(), AggregateOptions.<String, String>builder().load(ID).load(NAME).load(ABV).build());
        assertEquals(1, results.getCount());
        assertEquals(1000, results.size());
        assertEquals("harpoon ipa (2010)", ((String) results.get(999).get("name")).toLowerCase());
        assertEquals("0.086", results.get(9).get("abv"));
        results = sync.cursorRead(INDEX, results.getCursor());
        assertEquals(1000, results.size());
        String deleteStatus = sync.cursorDelete(INDEX, results.getCursor());
        assertEquals("OK", deleteStatus);
    }

}
