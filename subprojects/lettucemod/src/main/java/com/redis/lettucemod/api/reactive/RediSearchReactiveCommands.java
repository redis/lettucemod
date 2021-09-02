package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.api.search.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RediSearchReactiveCommands<K, V> {

    Mono<String> create(K index, Field... fields);

    Mono<String> create(K index, CreateOptions<K, V> options, Field... fields);

    Mono<String> dropindex(K index);

    Mono<String> dropindexDeleteDocs(K index);

    Mono<String> alter(K index, Field field);

    Flux<Object> indexInfo(K index);

    Mono<String> aliasadd(K name, K index);

    Mono<String> aliasupdate(K name, K index);

    Mono<String> aliasdel(K name);

    Flux<K> list();

    Mono<SearchResults<K, V>> search(K index, V query);

    Mono<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options);

    Mono<AggregateResults<K>> aggregate(K index, V query);

    Mono<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options);

    Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor);

    Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options);

    Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor);

    Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count);

    Mono<String> cursorDelete(K index, long cursor);

    Flux<V> tagvals(K index, K field);

    Mono<Long> sugadd(K key, V string, double score);

    Mono<Long> sugadd(K key, V string, double score, SugaddOptions<K, V> options);

    Flux<Suggestion<V>> sugget(K key, V prefix);

    Flux<Suggestion<V>> sugget(K key, V prefix, SuggetOptions options);

    Mono<Boolean> sugdel(K key, V string);

    Mono<Long> suglen(K key);

    @SuppressWarnings("unchecked")
    Mono<Long> dictadd(K dict, V... terms);

    @SuppressWarnings("unchecked")
    Mono<Long> dictdel(K dict, V... terms);

    Flux<V> dictdump(K dict);

}
