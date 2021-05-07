package com.redislabs.mesclun.search;

import io.lettuce.core.RedisFuture;

import java.util.List;

@SuppressWarnings("unchecked")
public interface RediSearchAsyncCommands<K, V> {

    RedisFuture<String> create(K index, Field<K>... fields);

    RedisFuture<String> create(K index, CreateOptions<K, V> options, Field<K>... fields);

    RedisFuture<String> dropIndex(K index);

    RedisFuture<String> dropIndex(K index, boolean deleteDocs);

    RedisFuture<String> alter(K index, Field<K> field);

    RedisFuture<List<Object>> indexInfo(K index);

    RedisFuture<String> aliasAdd(K name, K index);

    RedisFuture<String> aliasUpdate(K name, K index);

    RedisFuture<String> aliasDel(K name);

    RedisFuture<List<K>> list();

    RedisFuture<SearchResults<K, V>> search(K index, V query);

    RedisFuture<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options);

    RedisFuture<AggregateResults<K>> aggregate(K index, V query);

    RedisFuture<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options);

    RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor);

    RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options);

    RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor);

    RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count);

    RedisFuture<String> cursorDelete(K index, long cursor);

    RedisFuture<Long> sugadd(K key, V string, double score);

    RedisFuture<Long> sugadd(K key, V string, double score, SugaddOptions<V> options);

    RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix);

    RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix, SuggetOptions options);

    RedisFuture<Boolean> sugdel(K key, V string);

    RedisFuture<Long> suglen(K key);

}
