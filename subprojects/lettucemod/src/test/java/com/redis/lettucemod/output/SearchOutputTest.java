package com.redis.lettucemod.output;

import java.nio.ByteBuffer;

import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class SearchOutputTest {
    private SearchOutput<String, String> searchOutput = new SearchOutput<>(StringCodec.UTF8);

    @Test
    void parsesEmptyResponse() {
        searchOutput.multiArray(1);
        searchOutput.set(0);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(0));
        assertThat(result.getCount(), is(0L));
    }

    @Test
    void parsesOneDocument() {
        searchOutput.multiArray(3);
        searchOutput.set(1);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key1".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.multi(4);
        searchOutput.set(ByteBuffer.wrap("hashKey1".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue1".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashKey2".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue2".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(1));
        assertThat(result.getCount(), is(1L));
        Document<String, String> firstDocument = result.get(0);
        assertThat(firstDocument.getId(), is("key1"));
        assertThat(firstDocument, is(aMapWithSize(2)));
        assertThat(firstDocument, hasEntry("hashKey1", "hashValue1"));
        assertThat(firstDocument, hasEntry("hashKey2", "hashValue2"));
    }

    @Test
    void skipsDocumentWithNullContent() {
        searchOutput.multiArray(3);
        searchOutput.set(1);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key1".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(null);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, is(empty()));
        assertThat(result.getCount(), is(1L));
    }

    @Test
    void parsesDocumentAfterDocumentWithNullContent() {
        searchOutput.multiArray(5);
        searchOutput.set(2);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key1".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(null);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key2".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.multi(4);
        searchOutput.set(ByteBuffer.wrap("hashKey1".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue1".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashKey2".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue2".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(1));
        assertThat(result.getCount(), is(2L));
        Document<String, String> firstDocument = result.get(0);
        assertThat(firstDocument.getId(), is("key2"));
        assertThat(firstDocument, is(aMapWithSize(2)));
        assertThat(firstDocument, hasEntry("hashKey1", "hashValue1"));
        assertThat(firstDocument, hasEntry("hashKey2", "hashValue2"));
    }

    @Test
    void parsesWithScores_string() {
        searchOutput = new SearchOutput<>(StringCodec.UTF8, true, false, false);
        searchOutput.multiArray(4);
        searchOutput.set(1);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("0.3".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.multiArray(2);
        searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(1));
        assertThat(result.getCount(), is(1L));
        Document<String, String> firstDocument = result.get(0);
        assertThat(firstDocument.getId(), is("key"));
        assertThat(firstDocument.getScore(), is(0.3));
        assertThat(firstDocument, is(aMapWithSize(1)));
        assertThat(firstDocument, hasEntry("hashKey", "hashValue"));
    }

    @Test
    void parsesWithScores_double() {
        searchOutput = new SearchOutput<>(StringCodec.UTF8, true, false, false);
        searchOutput.multiArray(4);
        searchOutput.set(1);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(0.3d);
        searchOutput.complete(1);
        searchOutput.multiArray(2);
        searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(1));
        assertThat(result.getCount(), is(1L));
        Document<String, String> firstDocument = result.get(0);
        assertThat(firstDocument.getId(), is("key"));
        assertThat(firstDocument.getScore(), is(0.3));
        assertThat(firstDocument, is(aMapWithSize(1)));
        assertThat(firstDocument, hasEntry("hashKey", "hashValue"));
    }

    @Test
    void parsesWithSortKeys() {
        searchOutput = new SearchOutput<>(StringCodec.UTF8, false, true, false);
        searchOutput.multiArray(4);
        searchOutput.set(1);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("sortKey".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.multiArray(2);
        searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(1));
        assertThat(result.getCount(), is(1L));
        Document<String, String> firstDocument = result.get(0);
        assertThat(firstDocument.getId(), is("key"));
        assertThat(firstDocument.getSortKey(), is("sortKey"));
        assertThat(firstDocument, is(aMapWithSize(1)));
        assertThat(firstDocument, hasEntry("hashKey", "hashValue"));
    }

    @Test
    void parsesWithPayloads() {
        searchOutput = new SearchOutput<>(StringCodec.UTF8, false, false, true);
        searchOutput.multiArray(4);
        searchOutput.set(1);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("payload".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.multiArray(2);
        searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(1));
        assertThat(result.getCount(), is(1L));
        Document<String, String> firstDocument = result.get(0);
        assertThat(firstDocument.getId(), is("key"));
        assertThat(firstDocument.getPayload(), is("payload"));
        assertThat(firstDocument, is(aMapWithSize(1)));
        assertThat(firstDocument, hasEntry("hashKey", "hashValue"));
    }

    @Test
    void parseWithScoresSortKeyAndPayload() {
        searchOutput = new SearchOutput<>(StringCodec.UTF8, true, true, true);
        searchOutput.multiArray(6);
        searchOutput.set(1);
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("0.6".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("payload".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.set(ByteBuffer.wrap("sortKey".getBytes(UTF_8)));
        searchOutput.complete(1);
        searchOutput.multiArray(2);
        searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
        searchOutput.complete(2);
        searchOutput.complete(1);
        searchOutput.complete(0);

        SearchResults<String, String> result = searchOutput.get();
        assertThat(result, hasSize(1));
        assertThat(result.getCount(), is(1L));
        Document<String, String> firstDocument = result.get(0);
        assertThat(firstDocument.getId(), is("key"));
        assertThat(firstDocument.getScore(), is(0.6d));
        assertThat(firstDocument.getSortKey(), is("sortKey"));
        assertThat(firstDocument.getPayload(), is("payload"));
        assertThat(firstDocument, is(aMapWithSize(1)));
        assertThat(firstDocument, hasEntry("hashKey", "hashValue"));
    }
}