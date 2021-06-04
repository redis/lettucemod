package com.redislabs.mesclun.api.reactive;

import com.redislabs.mesclun.search.*;
import io.lettuce.core.RedisFuture;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface RediSearchReactiveCommands<K, V> {

    Mono<String> create(K index, Field... fields);

    Mono<String> create(K index, CreateOptions<K, V> options, Field... fields);

    Mono<String> dropIndex(K index);

    Mono<String> dropIndexDeleteDocs(K index);

    Mono<String> alter(K index, Field field);

    Flux<Object> indexInfo(K index);

    Mono<String> aliasAdd(K name, K index);

    Mono<String> aliasUpdate(K name, K index);

    Mono<String> aliasDel(K name);

    Flux<K> list();

    Mono<SearchResults<K, V>> search(K index, V query);

    Mono<SearchResults<K, V>> search(K index, V query, SearchOptions options);

    Mono<AggregateResults<K>> aggregate(K index, V query);

    Mono<AggregateResults<K>> aggregate(K index, V query, AggregateOptions options);

    Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor);

    Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions options);

    Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor);

    Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count);

    Mono<String> cursorDelete(K index, long cursor);

    Flux<V> tagVals(K index, K field);

    Mono<Long> sugadd(K key, V string, double score);

    Mono<Long> sugadd(K key, V string, double score, SugaddOptions<V> options);

    Flux<Suggestion<V>> sugget(K key, V prefix);

    Flux<Suggestion<V>> sugget(K key, V prefix, SuggetOptions options);

    Mono<Boolean> sugdel(K key, V string);

    Mono<Long> suglen(K key);

    Mono<Long> dictadd(K dict, V... terms);

    Mono<Long> dictdel(K dict, V... terms);

    Flux<V> dictdump(K dict);

}
