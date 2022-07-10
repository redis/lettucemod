package com.redis.lettucemod.api.async;

import io.lettuce.core.RedisFuture;

import java.util.List;

import com.redis.lettucemod.search.*;

public interface RediSearchAsyncCommands<K, V> {

    RedisFuture<String> ftCreate(K index, Field... fields);

    RedisFuture<String> ftCreate(K index, CreateOptions<K, V> options, Field... fields);

    RedisFuture<String> ftDropindex(K index);

    RedisFuture<String> ftDropindexDeleteDocs(K index);

    RedisFuture<String> ftAlter(K index, Field field);

    RedisFuture<List<Object>> ftInfo(K index);

    RedisFuture<String> ftAliasadd(K name, K index);

    RedisFuture<String> ftAliasupdate(K name, K index);

    RedisFuture<String> ftAliasdel(K name);

    RedisFuture<List<K>> ftList();

    RedisFuture<SearchResults<K, V>> ftSearch(K index, V query);

    RedisFuture<SearchResults<K, V>> ftSearch(K index, V query, SearchOptions<K, V> options);

    RedisFuture<AggregateResults<K>> ftAggregate(K index, V query);

    RedisFuture<AggregateResults<K>> ftAggregate(K index, V query, AggregateOptions<K, V> options);

    RedisFuture<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor);

    RedisFuture<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor, AggregateOptions<K, V> options);

    RedisFuture<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor);

    RedisFuture<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor, long count);

    RedisFuture<String> ftCursorDelete(K index, long cursor);

    RedisFuture<List<V>> ftTagvals(K index, K field);

    RedisFuture<Long> ftSugadd(K key, Suggestion<V> suggestion);

    RedisFuture<Long> ftSugaddIncr(K key, Suggestion<V> suggestion);

    RedisFuture<List<Suggestion<V>>> ftSugget(K key, V prefix);

    RedisFuture<List<Suggestion<V>>> ftSugget(K key, V prefix, SuggetOptions options);

    RedisFuture<Boolean> ftSugdel(K key, V string);

    RedisFuture<Long> ftSuglen(K key);

    @SuppressWarnings("unchecked")
    RedisFuture<Long> ftDictadd(K dict, V... terms);

    @SuppressWarnings("unchecked")
    RedisFuture<Long> ftDictdel(K dict, V... terms);

    RedisFuture<List<V>> ftDictdump(K dict);

}
