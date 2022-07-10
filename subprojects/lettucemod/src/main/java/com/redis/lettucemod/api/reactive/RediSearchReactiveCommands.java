package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RediSearchReactiveCommands<K, V> {

    Mono<String> ftCreate(K index, Field... fields);

    Mono<String> ftCreate(K index, CreateOptions<K, V> options, Field... fields);

    Mono<String> ftDropindex(K index);

    Mono<String> ftDropindexDeleteDocs(K index);

    Mono<String> ftAlter(K index, Field field);

    Flux<Object> ftInfo(K index);

    Mono<String> ftAliasadd(K name, K index);

    Mono<String> ftAliasupdate(K name, K index);

    Mono<String> ftAliasdel(K name);

    Flux<K> ftList();

    Mono<SearchResults<K, V>> ftSearch(K index, V query);

    Mono<SearchResults<K, V>> ftSearch(K index, V query, SearchOptions<K, V> options);

    Mono<AggregateResults<K>> ftAggregate(K index, V query);

    Mono<AggregateResults<K>> ftAggregate(K index, V query, AggregateOptions<K, V> options);

    Mono<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor);

    Mono<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor, AggregateOptions<K, V> options);

    Mono<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor);

    Mono<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor, long count);

    Mono<String> ftCursorDelete(K index, long cursor);

    Flux<V> ftTagvals(K index, K field);

    Mono<Long> ftSugadd(K key, Suggestion<V> suggestion);

    Mono<Long> ftSugaddIncr(K key, Suggestion<V> suggestion);

    Flux<Suggestion<V>> ftSugget(K key, V prefix);

    Flux<Suggestion<V>> ftSugget(K key, V prefix, SuggetOptions options);

    Mono<Boolean> ftSugdel(K key, V string);

    Mono<Long> ftSuglen(K key);

    @SuppressWarnings("unchecked")
    Mono<Long> ftDictadd(K dict, V... terms);

    @SuppressWarnings("unchecked")
    Mono<Long> ftDictdel(K dict, V... terms);

    Flux<V> ftDictdump(K dict);

}
