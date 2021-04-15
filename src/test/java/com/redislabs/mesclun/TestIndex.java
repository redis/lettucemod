package com.redislabs.mesclun;

import com.redislabs.mesclun.search.*;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("unchecked")
public class TestIndex extends AbstractSearchTest {

    @Test
    public void temporary() throws InterruptedException {
        String indexName = "temporaryIndex";
        sync.create(indexName, CreateOptions.<String, String>builder().temporary(1L).build(), Field.text("field1").build());

        List<Object> info = sync.ftInfo(indexName);
        assertEquals(indexName, info.get(1));
        Thread.sleep(1501);
        try {
            sync.ftInfo(indexName);
        } catch (RedisCommandExecutionException e) {
            assertEquals("Unknown Index name", e.getMessage());
            return;
        }
        fail("Temporary index not deleted");
    }

    @Test
    public void dropIndex() throws InterruptedException, IOException {
        createBeerIndex();
        sync.dropIndex(INDEX, false);
        // allow some time for the index to be deleted
        Thread.sleep(100);
        try {
            sync.search(INDEX, "*");
            fail("Index not dropped");
        } catch (RedisCommandExecutionException e) {
            // ignored, expected behavior
        }
    }

    @Test
    public void alter() throws IOException {
        createBeerIndex();
        sync.alter(INDEX, Field.tag("newField").build());
        Map<String, String> doc = mapOf("newField", "value1");
        sync.hmset("beer:newDoc", doc);
        SearchResults<String, String> results = sync.search(INDEX, "@newField:{value1}");
        assertEquals(1, results.getCount());
        assertEquals(doc.get("newField"), results.get(0).get("newField"));
    }

    @Test
    public void info() throws IOException {
        createBeerIndex();
        Map<String, Object> indexInfo = toMap(sync.ftInfo(INDEX));
        assertEquals(INDEX, indexInfo.get("index_name"));
    }

    @Test
    public void testCreateOptions() {
        CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:").payloadField("xml").build();
        Field<String, String>[] fields = new Field[]{Field.text("artist").sortable(true).build(), Field.tag("id").sortable(true).build(), Field.text("title").sortable(true).build()};
        sync.create("releases", options, fields);
        IndexInfo<String, String> info = RediSearchUtils.getInfo(sync.ftInfo("releases"));
        Assertions.assertEquals(fields.length, info.getFields().size());

    }

    private Map<String, Object> toMap(List<Object> indexInfo) {
        Map<String, Object> map = new HashMap<>();
        Iterator<Object> iterator = indexInfo.iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            map.put(key, iterator.next());
        }
        return map;
    }

    @Test
    public void createOnHash() throws IOException, InterruptedException {
        sync.flushall();
        Thread.sleep(1000);
        String indexName = "hashIndex";
        sync.create(indexName, CreateOptions.<String, String>builder().prefix("beer:").on(CreateOptions.Structure.HASH).build(), SCHEMA);
        List<Map<String, String>> beers = beers();
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (Map<String, String> beer : beers) {
            String id = beer.get(ID);
            futures.add(async.hmset("beer:" + id, beer));
        }
        async.flushCommands();
        LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
        Thread.sleep(1000);
        async.setAutoFlushCommands(true);
        IndexInfo<String, String> info = RediSearchUtils.getInfo(sync.ftInfo(indexName));
        Double numDocs = info.getNumDocs();
        assertEquals(2348, numDocs);
    }

    @Test
    public void list() {
        sync.flushall();
        List<String> indexNames = Arrays.asList("index1", "index2", "index3");
        for (String indexName : indexNames) {
            sync.create(indexName, Field.text("field1").sortable(true).build());
        }
        List<String> list = sync.list();
        assertEquals(new HashSet<>(indexNames), new HashSet<>(list));
    }

}
