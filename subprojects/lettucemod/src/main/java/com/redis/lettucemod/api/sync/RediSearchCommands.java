package com.redis.lettucemod.api.sync;

import com.redis.lettucemod.search.*;

import java.util.List;

public interface RediSearchCommands<K, V> {

    String create(K index, Field... fields);

    String create(K index, CreateOptions<K, V> options, Field... fields);

    String dropIndex(K index);

    String dropIndexDeleteDocs(K index);

    String alter(K index, Field field);

    List<Object> indexInfo(K index);

    String aliasAdd(K name, K index);

    String aliasUpdate(K name, K index);

    String aliasDel(K name);

    List<K> list();

    SearchResults<K, V> search(K index, V query);

    SearchResults<K, V> search(K index, V query, SearchOptions<K, V> options);

    AggregateResults<K> aggregate(K index, V query);

    AggregateResults<K> aggregate(K index, V query, AggregateOptions<K, V> options);

    AggregateWithCursorResults<K> aggregate(K index, V query, Cursor cursor);

    AggregateWithCursorResults<K> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options);

    AggregateWithCursorResults<K> cursorRead(K index, long cursor);

    AggregateWithCursorResults<K> cursorRead(K index, long cursor, long count);

    String cursorDelete(K index, long cursor);

    List<V> tagVals(K index, K field);

    Long sugadd(K key, V string, double score);

    Long sugadd(K key, V string, double score, SugaddOptions<K, V> options);

    List<Suggestion<V>> sugget(K key, V prefix);

    List<Suggestion<V>> sugget(K key, V prefix, SuggetOptions options);

    Boolean sugdel(K key, V string);

    Long suglen(K key);

    @SuppressWarnings("unchecked")
    Long dictadd(K dict, V... terms);

    @SuppressWarnings("unchecked")
    Long dictdel(K dict, V... terms);

    List<V> dictdump(K dict);
}
