package com.redislabs.mesclun;

import com.redislabs.mesclun.search.Suggestion;
import com.redislabs.mesclun.search.SuggetOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSuggest extends AbstractSearchTest {

    @Test
    public void testSugget() throws IOException {
        createBeerSuggestions();
        List<Suggestion<String>> results = sync.sugget(SUGINDEX, "Ame");
        assertEquals(5, results.size());
    }

    @Test
    public void testSuggetOptions() throws IOException {
        createBeerSuggestions();
        List<Suggestion<String>> results = sync.sugget(SUGINDEX, "Ame", SuggetOptions.builder().max(1000L).build());
        assertEquals(8, results.size());
    }

    @Test
    public void testSuggetWithScores() throws IOException {
        createBeerSuggestions();
        List<Suggestion<String>> results = sync.sugget(SUGINDEX, "Ame", SuggetOptions.builder().max(1000L).withScores(true).build());
        assertEquals(8, results.size());
        assertEquals("American Hero", results.get(0).getString());
    }

    @Test
    public void testSuglen() throws IOException {
        createBeerSuggestions();
        long length = sync.suglen(SUGINDEX);
        assertEquals(2305, length);
    }

    @Test
    public void testSugdel() throws IOException {
        createBeerSuggestions();
        Boolean result = sync.sugdel(SUGINDEX, "American Hero");
        assertTrue(result);
    }

}
