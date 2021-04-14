package com.redislabs.mesclun.search;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public interface RediSearchReactiveCommands<K, V> {

    Mono<String> create(K index, Field<K>... fields);

    Mono<String> create(K index, CreateOptions<K, V> options, Field<K>... fields);

    Mono<String> dropIndex(K index);

    Mono<String> dropIndex(K index, boolean deleteDocs);

    Mono<String> alter(K index, Field<K> field);

    Flux<Object> ftInfo(K index);

    Mono<String> aliasAdd(K name, K index);

    Mono<String> aliasUpdate(K name, K index);

    Mono<String> aliasDel(K name);

    Flux<K> list();

    Mono<SearchResults<K, V>> search(K index, V query);

    Mono<SearchResults<K, V>> search(K index, V query, SearchOptions<K> options);

    Mono<AggregateResults<K>> aggregate(K index, V query);

    Mono<AggregateResults<K>> aggregate(K index, V query, AggregateOptions options);

    Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor);

    Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions options);

    Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor);

    Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count);

    Mono<String> cursorDelete(K index, long cursor);

    Mono<Long> sugadd(K key, Suggestion<V> suggestion);

    Mono<Long> sugadd(K key, Suggestion<V> suggestion, boolean increment);

    Flux<Suggestion<V>> sugget(K key, V prefix);

    Flux<Suggestion<V>> sugget(K key, V prefix, SuggetOptions options);

    Mono<Boolean> sugdel(K key, V string);

    Mono<Long> suglen(K key);

}