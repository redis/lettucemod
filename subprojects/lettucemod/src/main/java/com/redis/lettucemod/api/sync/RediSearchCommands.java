package com.redis.lettucemod.api.sync;

import java.util.List;

import com.redis.lettucemod.search.*;

public interface RediSearchCommands<K, V> {

    String create(K index, Field... fields);

    String create(K index, CreateOptions<K, V> options, Field... fields);

    String dropindex(K index);

    String dropindexDeleteDocs(K index);

    String alter(K index, Field field);

    List<Object> indexInfo(K index);

    String aliasadd(K name, K index);

    String aliasupdate(K name, K index);

    String aliasdel(K name);

    /**
     * 
     * @return List of RediSearch indexes 
     */
    List<K> list();

    SearchResults<K, V> search(K index, V query);

    SearchResults<K, V> search(K index, V query, SearchOptions<K, V> options);

    AggregateResults<K> aggregate(K index, V query);

    AggregateResults<K> aggregate(K index, V query, AggregateOptions<K, V> options);

    AggregateWithCursorResults<K> aggregate(K index, V query, CursorOptions cursor);

    AggregateWithCursorResults<K> aggregate(K index, V query, CursorOptions cursor, AggregateOptions<K, V> options);

    AggregateWithCursorResults<K> cursorRead(K index, long cursor);

    AggregateWithCursorResults<K> cursorRead(K index, long cursor, long count);

    String cursorDelete(K index, long cursor);

    List<V> tagvals(K index, K field);

    Long sugadd(K key, V string, double score);

    Long sugaddIncr(K key, V string, double score);

    Long sugadd(K key, V string, double score, V payload);

    Long sugaddIncr(K key, V string, double score, V payload);

    Long sugadd(K key, Suggestion<V> suggestion);

    Long sugaddIncr(K key, Suggestion<V> suggestion);

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
