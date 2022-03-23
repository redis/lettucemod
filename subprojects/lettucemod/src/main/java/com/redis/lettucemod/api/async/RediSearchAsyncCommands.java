package com.redis.lettucemod.api.async;

import io.lettuce.core.RedisFuture;

import java.util.List;

import com.redis.lettucemod.search.*;

public interface RediSearchAsyncCommands<K, V> {

    RedisFuture<String> create(K index, Field... fields);

    RedisFuture<String> create(K index, CreateOptions<K, V> options, Field... fields);

    RedisFuture<String> dropindex(K index);

    RedisFuture<String> dropindexDeleteDocs(K index);

    RedisFuture<String> alter(K index, Field field);

    RedisFuture<List<Object>> indexInfo(K index);

    RedisFuture<String> aliasadd(K name, K index);

    RedisFuture<String> aliasupdate(K name, K index);

    RedisFuture<String> aliasdel(K name);

    RedisFuture<List<K>> list();

    RedisFuture<SearchResults<K, V>> search(K index, V query);

    RedisFuture<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options);

    RedisFuture<AggregateResults<K>> aggregate(K index, V query);

    RedisFuture<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options);

    RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, CursorOptions cursor);

    RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, CursorOptions cursor, AggregateOptions<K, V> options);

    RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor);

    RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count);

    RedisFuture<String> cursorDelete(K index, long cursor);

    RedisFuture<List<V>> tagvals(K index, K field);

    RedisFuture<Long> sugadd(K key, V string, double score);

    RedisFuture<Long> sugaddIncr(K key, V string, double score);

    RedisFuture<Long> sugadd(K key, V string, double score, V payload);

    RedisFuture<Long> sugaddIncr(K key, V string, double score, V payload);

    RedisFuture<Long> sugadd(K key, Suggestion<V> suggestion);

    RedisFuture<Long> sugaddIncr(K key, Suggestion<V> suggestion);

    RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix);

    RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix, SuggetOptions options);

    RedisFuture<Boolean> sugdel(K key, V string);

    RedisFuture<Long> suglen(K key);

    @SuppressWarnings("unchecked")
    RedisFuture<Long> dictadd(K dict, V... terms);

    @SuppressWarnings("unchecked")
    RedisFuture<Long> dictdel(K dict, V... terms);

    RedisFuture<List<V>> dictdump(K dict);

}
