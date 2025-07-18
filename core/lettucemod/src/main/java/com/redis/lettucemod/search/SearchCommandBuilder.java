package com.redis.lettucemod.search;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.redis.lettucemod.RedisModulesCommandBuilder;
import com.redis.lettucemod.output.AggregateOutput;
import com.redis.lettucemod.output.AggregateWithCursorOutput;
import com.redis.lettucemod.output.SearchNoContentOutput;
import com.redis.lettucemod.output.SearchOutput;
import com.redis.lettucemod.output.SuggetOutput;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandType;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.BooleanOutput;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.KeyListOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueListOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

/**
 * Builder dedicated to RediSearch commands.
 */
public class SearchCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

    public SearchCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected <A, B, T> Command<A, B, T> createCommand(SearchCommandType type, CommandOutput<A, B, T> output,
            CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    private static void notNullIndex(Object index) {
        notNull(index, "Index");
    }

    @SuppressWarnings("unchecked")
    public Command<K, V, String> create(K index, CreateOptions<K, V> options, Field<K>... fields) {
        notNullIndex(index);
        LettuceAssert.isTrue(fields.length > 0, "At least one field is required.");
        SearchCommandArgs<K, V> args = args(index);
        if (options != null) {
            options.build(args);
        }
        args.add(SearchCommandKeyword.SCHEMA);
        for (Field<K> field : fields) {
            field.build(args);
        }
        return createCommand(SearchCommandType.CREATE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> dropIndex(K index, boolean deleteDocs) {
        notNullIndex(index);
        SearchCommandArgs<K, V> args = args(index);
        if (deleteDocs) {
            args.add(SearchCommandKeyword.DD);
        }
        return createCommand(SearchCommandType.DROPINDEX, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Object>> info(K index) {
        notNullIndex(index);
        SearchCommandArgs<K, V> args = args(index);
        return createCommand(SearchCommandType.INFO, new NestedMultiOutput<>(codec), args);
    }

    public Command<K, V, String> alter(K index, Field<K> field) {
        notNullIndex(index);
        notNull(field, "Field");
        SearchCommandArgs<K, V> args = args(index);
        args.add(SearchCommandKeyword.SCHEMA);
        args.add(SearchCommandKeyword.ADD);
        field.build(args);
        return createCommand(SearchCommandType.ALTER, new StatusOutput<>(codec), args);
    }

    @Override
    protected SearchCommandArgs<K, V> args(K key) {
        return new SearchCommandArgs<>(codec).addKey(key);
    }

    private static void notNullQuery(Object query) {
        notNull(query, "Query");
    }

    public Command<K, V, SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        notNullIndex(index);
        notNullQuery(query);
        SearchCommandArgs<K, V> args = args(index);
        args.addValue(query);
        if (options != null) {
            options.build(args);
        }
        return createCommand(SearchCommandType.SEARCH, searchOutput(options), args);
    }

    private CommandOutput<K, V, SearchResults<K, V>> searchOutput(SearchOptions<K, V> options) {
        if (options == null) {
            return new SearchOutput<>(codec);
        }
        if (options.isNoContent()) {
            return new SearchNoContentOutput<>(codec, options.isWithScores());
        }
        return new SearchOutput<>(codec, options.isWithScores(), options.isWithSortKeys(), options.isWithPayloads());
    }

    public Command<K, V, SearchResults<K, V>> search(K index, V query, V... options) {
        notNullIndex(index);
        notNullQuery(query);
        SearchCommandArgs<K, V> args = args(index);
        args.addValue(query);
        SearchOptions<K, V> searchOptions = new SearchOptions<>();
        args.addValues(options);
        for (V option : options) {
            String optionString = StringCodec.UTF8.decodeValue(codec.encodeValue(option));
            if (SearchCommandKeyword.WITHSORTKEYS.name().equalsIgnoreCase(optionString)) {
                searchOptions.setWithSortKeys(true);
            }
            if (SearchCommandKeyword.WITHPAYLOADS.name().equalsIgnoreCase(optionString)) {
                searchOptions.setWithPayloads(true);
            }
            if (SearchCommandKeyword.WITHSCORES.name().equalsIgnoreCase(optionString)) {
                searchOptions.setWithScores(true);
            }
        }
        return createCommand(SearchCommandType.SEARCH, searchOutput(searchOptions), args);
    }

    public Command<K, V, AggregateResults<K>> aggregate(K index, V query, V... options) {
        return doAggregate(index, query, args -> args.addValues(options));
    }

    public Command<K, V, AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options) {
        return doAggregate(index, query, options == null ? null : options::build);
    }

    private Command<K, V, AggregateResults<K>> doAggregate(K index, V query, Consumer<SearchCommandArgs<K, V>> options) {
        notNullIndex(index);
        notNullQuery(query);
        SearchCommandArgs<K, V> args = args(index);
        args.addValue(query);
        if (options != null) {
            options.accept(args);
        }
        return createCommand(SearchCommandType.AGGREGATE, new AggregateOutput<>(codec, new AggregateResults<>()), args);
    }

    public Command<K, V, AggregateWithCursorResults<K>> aggregate(K index, V query, CursorOptions cursor,
            AggregateOptions<K, V> options) {
        notNullIndex(index);
        notNullQuery(query);
        SearchCommandArgs<K, V> args = args(index);
        args.addValue(query);
        if (options != null) {
            options.build(args);
        }
        args.add(SearchCommandKeyword.WITHCURSOR);
        if (cursor != null) {
            cursor.build(args);
        }
        return createCommand(SearchCommandType.AGGREGATE, new AggregateWithCursorOutput<>(codec), args);
    }

    public Command<K, V, AggregateWithCursorResults<K>> cursorRead(K index, long cursor, Long count) {
        notNullIndex(index);
        SearchCommandArgs<K, V> args = new SearchCommandArgs<>(codec);
        args.add(SearchCommandKeyword.READ);
        args.addKey(index);
        args.add(cursor);
        if (count != null) {
            args.add(SearchCommandKeyword.COUNT);
            args.add(count);
        }
        return createCommand(SearchCommandType.CURSOR, new AggregateWithCursorOutput<>(codec), args);
    }

    public Command<K, V, String> cursorDelete(K index, long cursor) {
        notNullIndex(index);
        SearchCommandArgs<K, V> args = new SearchCommandArgs<>(codec);
        args.add(SearchCommandKeyword.DEL);
        args.addKey(index);
        args.add(cursor);
        return createCommand(SearchCommandType.CURSOR, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<V>> tagVals(K index, K field) {
        notNullIndex(index);
        SearchCommandArgs<K, V> args = args(index);
        args.addKey(field);
        return createCommand(SearchCommandType.TAGVALS, new ValueListOutput<>(codec), args);
    }

    private static void notNullDict(Object dict) {
        notNull(dict, "Dict");
    }

    @SuppressWarnings("unchecked")
    public Command<K, V, Long> dictadd(K dict, V... terms) {
        notNullDict(dict);
        return createCommand(SearchCommandType.DICTADD, new IntegerOutput<>(codec), args(dict).addValues(terms));
    }

    @SuppressWarnings("unchecked")
    public Command<K, V, Long> dictdel(K dict, V... terms) {
        notNullDict(dict);
        return createCommand(SearchCommandType.DICTDEL, new IntegerOutput<>(codec), args(dict).addValues(terms));
    }

    public Command<K, V, List<V>> dictdump(K dict) {
        notNullDict(dict);
        return createCommand(SearchCommandType.DICTDUMP, new ValueListOutput<>(codec), args(dict));
    }

    public Command<K, V, Long> sugadd(K key, V string, double score) {
        return sugadd(key, string, score, null, false);
    }

    public Command<K, V, Long> sugaddIncr(K key, V string, double score) {
        return sugadd(key, string, score, null, true);
    }

    public Command<K, V, Long> sugadd(K key, V string, double score, V payload) {
        return sugadd(key, string, score, payload, false);
    }

    public Command<K, V, Long> sugaddIncr(K key, V string, double score, V payload) {
        return sugadd(key, string, score, payload, true);
    }

    public Command<K, V, Long> sugadd(K key, V string, double score, V payload, boolean increment) {
        notNullKey(key);
        notNull(string, "String");
        SearchCommandArgs<K, V> args = args(key);
        args.addValue(string);
        args.add(score);
        if (increment) {
            args.add(SearchCommandKeyword.INCR);
        }
        if (payload != null) {
            args.add(SearchCommandKeyword.PAYLOAD);
            args.addValue(payload);
        }
        return createCommand(SearchCommandType.SUGADD, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> sugadd(K key, Suggestion<V> suggestion) {
        notNull(suggestion, "Suggestion");
        return sugadd(key, suggestion.getString(), suggestion.getScore(), suggestion.getPayload(), false);
    }

    public Command<K, V, Long> sugaddIncr(K key, Suggestion<V> suggestion) {
        notNull(suggestion, "Suggestion");
        return sugadd(key, suggestion.getString(), suggestion.getScore(), suggestion.getPayload(), true);
    }

    public Command<K, V, List<Suggestion<V>>> sugget(K key, V prefix) {
        return sugget(key, prefix, null);
    }

    public Command<K, V, List<Suggestion<V>>> sugget(K key, V prefix, SuggetOptions options) {
        notNullKey(key);
        notNull(prefix, "Prefix");
        SearchCommandArgs<K, V> args = args(key);
        args.addValue(prefix);
        if (options != null) {
            options.build(args);
        }
        return createCommand(SearchCommandType.SUGGET, suggetOutput(options), args);
    }

    private SuggetOutput<K, V> suggetOutput(SuggetOptions options) {
        if (options == null) {
            return new SuggetOutput<>(codec);
        }
        return new SuggetOutput<>(codec, options.isWithScores(), options.isWithPayloads());
    }

    public Command<K, V, Boolean> sugdel(K key, V string) {
        notNullKey(key);
        notNull(string, "String");
        return createCommand(SearchCommandType.SUGDEL, new BooleanOutput<>(codec), args(key).addValue(string));
    }

    public Command<K, V, Long> suglen(K key) {
        notNullKey(key);
        return createCommand(SearchCommandType.SUGLEN, new IntegerOutput<>(codec), args(key));

    }

    private static void notNullName(Object name) {
        notNull(name, "Name");
    }

    public Command<K, V, String> aliasAdd(K name, K index) {
        notNullName(name);
        notNullIndex(index);
        return createCommand(SearchCommandType.ALIASADD, new StatusOutput<>(codec), args(name).addKey(index));
    }

    public Command<K, V, String> aliasUpdate(K name, K index) {
        notNullName(name);
        notNullIndex(index);
        return createCommand(SearchCommandType.ALIASUPDATE, new StatusOutput<>(codec), args(name).addKey(index));
    }

    public Command<K, V, String> aliasDel(K name) {
        notNullName(name);
        return createCommand(SearchCommandType.ALIASDEL, new StatusOutput<>(codec), args(name));
    }

    public Command<K, V, List<K>> list() {
        return new Command<>(SearchCommandType.LIST, new KeyListOutput<>(codec));
    }

}
