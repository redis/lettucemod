package com.redis.lettucemod.api.sync;

import java.util.List;

import com.redis.lettucemod.search.*;

public interface RediSearchCommands<K, V> {

	@SuppressWarnings("unchecked")
	String ftCreate(K index, Field<K>... fields);

	@SuppressWarnings("unchecked")
	String ftCreate(K index, CreateOptions<K, V> options, Field<K>... fields);

	String ftDropindex(K index);

	String ftDropindexDeleteDocs(K index);

	String ftAlter(K index, Field<K> field);

	List<Object> ftInfo(K index);

	String ftAliasadd(K name, K index);

	String ftAliasupdate(K name, K index);

	String ftAliasdel(K name);

	/**
	 * 
	 * @return List of RediSearch indexes
	 */
	List<K> ftList();

	SearchResults<K, V> ftSearch(K index, V query, V... options);

	SearchResults<K, V> ftSearch(K index, V query, SearchOptions<K, V> options);

	AggregateResults<K> ftAggregate(K index, V query, V... options);

	AggregateResults<K> ftAggregate(K index, V query, AggregateOptions<K, V> options);

	AggregateWithCursorResults<K> ftAggregate(K index, V query, CursorOptions cursor);

	AggregateWithCursorResults<K> ftAggregate(K index, V query, CursorOptions cursor, AggregateOptions<K, V> options);

	AggregateWithCursorResults<K> ftCursorRead(K index, long cursor);

	AggregateWithCursorResults<K> ftCursorRead(K index, long cursor, long count);

	String ftCursorDelete(K index, long cursor);

	List<V> ftTagvals(K index, K field);

	Long ftSugadd(K key, Suggestion<V> suggestion);

	Long ftSugaddIncr(K key, Suggestion<V> suggestion);

	List<Suggestion<V>> ftSugget(K key, V prefix);

	List<Suggestion<V>> ftSugget(K key, V prefix, SuggetOptions options);

	Boolean ftSugdel(K key, V string);

	Long ftSuglen(K key);

	@SuppressWarnings("unchecked")
	Long ftDictadd(K dict, V... terms);

	@SuppressWarnings("unchecked")
	Long ftDictdel(K dict, V... terms);

	List<V> ftDictdump(K dict);
}
