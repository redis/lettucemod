package com.redislabs.mesclun.api.async;

import com.redislabs.mesclun.search.*;
import io.lettuce.core.RedisFuture;

import java.util.List;

public interface RediSearchAsyncCommands<K, V> {

    RedisFuture<String> create(K index, Field... fields);

    RedisFuture<String> create(K index, CreateOptions<K, V> options, Field... fields);

    RedisFuture<String> dropIndex(K index);

    RedisFuture<String> dropIndexDeleteDocs(K index);

    RedisFuture<String> alter(K index, Field field);

    RedisFuture<List<Object>> indexInfo(K index);

    RedisFuture<String> aliasAdd(K name, K index);

    RedisFuture<String> aliasUpdate(K name, K index);

    RedisFuture<String> aliasDel(K name);

    RedisFuture<List<K>> list();

    RedisFuture<SearchResults<K, V>> search(K index, V query);

    RedisFuture<SearchResults<K, V>> search(K index, V query, SearchOptions options);

    RedisFuture<AggregateResults<K>> aggregate(K index, V query);

    RedisFuture<AggregateResults<K>> aggregate(K index, V query, AggregateOptions options);

    RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor);

    RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions options);

    RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor);

    RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count);

    RedisFuture<String> cursorDelete(K index, long cursor);

    RedisFuture<List<V>> tagVals(K index, K field);

    RedisFuture<Long> sugadd(K key, V string, double score);

    RedisFuture<Long> sugadd(K key, V string, double score, SugaddOptions<V> options);

    RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix);

    RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix, SuggetOptions options);

    RedisFuture<Boolean> sugdel(K key, V string);

    RedisFuture<Long> suglen(K key);

    RedisFuture<Long> dictadd(K dict, V... terms);

    RedisFuture<Long> dictdel(K dict, V... terms);

    RedisFuture<List<V>> dictdump(K dict);

}
