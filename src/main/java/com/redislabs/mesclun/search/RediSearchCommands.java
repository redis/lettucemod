package com.redislabs.mesclun.search;

import java.util.List;

@SuppressWarnings("unchecked")
public interface RediSearchCommands<K, V> {

    String create(K index, Field<K>... fields);

    String create(K index, CreateOptions<K, V> options, Field<K>... fields);

    String dropIndex(K index);

    String dropIndex(K index, boolean deleteDocs);

    String alter(K index, Field<K> field);

    List<Object> ftInfo(K index);

    String aliasAdd(K name, K index);

    String aliasUpdate(K name, K index);

    String aliasDel(K name);

    List<K> list();

    SearchResults<K, V> search(K index, V query);

    SearchResults<K, V> search(K index, V query, SearchOptions<K> options);

    AggregateResults<K> aggregate(K index, V query);

    AggregateResults<K> aggregate(K index, V query, AggregateOptions options);

    AggregateWithCursorResults<K> aggregate(K index, V query, Cursor cursor);

    AggregateWithCursorResults<K> aggregate(K index, V query, Cursor cursor, AggregateOptions options);

    AggregateWithCursorResults<K> cursorRead(K index, long cursor);

    AggregateWithCursorResults<K> cursorRead(K index, long cursor, long count);

    String cursorDelete(K index, long cursor);

    Long sugadd(K key, Suggestion<V> suggestion);

    Long sugadd(K key, Suggestion<V> suggestion, boolean increment);

    List<Suggestion<V>> sugget(K key, V prefix);

    List<Suggestion<V>> sugget(K key, V prefix, SuggetOptions options);

    Boolean sugdel(K key, V string);

    Long suglen(K key);
}
