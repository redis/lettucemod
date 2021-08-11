package com.redislabs.lettucemod.search;

import com.redislabs.lettucemod.RedisModulesCommandBuilder;
import com.redislabs.lettucemod.search.output.*;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.CommandType;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

/**
 * Dedicated pub/sub command builder to build pub/sub commands.
 */
public class RediSearchCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

    public RediSearchCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected <A, B, T> Command<A, B, T> createCommand(CommandType type, CommandOutput<A, B, T> output, CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    public Command<K, V, String> create(K index, CreateOptions<K, V> options, Field... fields) {
        notNull(index, "index");
        LettuceAssert.isTrue(fields.length > 0, "At least one field is required.");
        RediSearchCommandArgs<K, V> args = args(index);
        if (options != null) {
            options.build(args);
        }
        args.add(CommandKeyword.SCHEMA);
        for (Field field : fields) {
            field.build(args);
        }
        return createCommand(CommandType.CREATE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> dropIndex(K index, boolean deleteDocs) {
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = args(index);
        if (deleteDocs) {
            args.add(CommandKeyword.DD);
        }
        return createCommand(CommandType.DROPINDEX, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Object>> info(K index) {
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = args(index);
        return createCommand(CommandType.INFO, new NestedMultiOutput<>(codec), args);
    }

    public Command<K, V, String> alter(K index, Field field) {
        notNull(index, "index");
        notNull(field, "field");
        RediSearchCommandArgs<K, V> args = args(index);
        args.add(CommandKeyword.SCHEMA);
        args.add(CommandKeyword.ADD);
        field.build(args);
        return createCommand(CommandType.ALTER, new StatusOutput<>(codec), args);
    }

    private RediSearchCommandArgs<K, V> args(K key) {
        return new RediSearchCommandArgs<>(codec).addKey(key);
    }

    public Command<K, V, SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        notNull(index, "index");
        notNull(query, "query");
        RediSearchCommandArgs<K, V> args = args(index);
        args.addValue(query);
        if (options != null) {
            options.build(args);
        }
        return createCommand(CommandType.SEARCH, searchOutput(options), args);
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

    public Command<K, V, AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options) {
        notNull(index, "index");
        notNull(query, "query");
        RediSearchCommandArgs<K, V> args = args(index);
        args.addValue(query);
        if (options != null) {
            options.build(args);
        }
        return createCommand(CommandType.AGGREGATE, new AggregateOutput<>(codec, new AggregateResults<>()), args);
    }

    public Command<K, V, AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options) {
        notNull(index, "index");
        notNull(query, "query");
        RediSearchCommandArgs<K, V> args = args(index);
        args.addValue(query);
        if (options != null) {
            options.build(args);
        }
        args.add(CommandKeyword.WITHCURSOR);
        if (cursor != null) {
            cursor.build(args);
        }
        return createCommand(CommandType.AGGREGATE, new AggregateWithCursorOutput<>(codec), args);
    }

    public Command<K, V, AggregateWithCursorResults<K>> cursorRead(K index, long cursor, Long count) {
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec);
        args.add(CommandKeyword.READ);
        args.addKey(index);
        args.add(cursor);
        if (count != null) {
            args.add(CommandKeyword.COUNT);
            args.add(count);
        }
        return createCommand(CommandType.CURSOR, new AggregateWithCursorOutput<>(codec), args);
    }

    public Command<K, V, String> cursorDelete(K index, long cursor) {
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec);
        args.add(CommandKeyword.DEL);
        args.addKey(index);
        args.add(cursor);
        return createCommand(CommandType.CURSOR, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<V>> tagVals(K index, K field) {
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = args(index);
        args.addKey(field);
        return createCommand(CommandType.TAGVALS, new ValueListOutput<>(codec), args);
    }

    @SuppressWarnings("unchecked")
    public Command<K, V, Long> dictadd(K dict, V... terms) {
        notNull(dict, "dict");
        return createCommand(CommandType.DICTADD, new IntegerOutput<>(codec), args(dict).addValues(terms));
    }

    @SuppressWarnings("unchecked")
    public Command<K, V, Long> dictdel(K dict, V... terms) {
        notNull(dict, "dict");
        return createCommand(CommandType.DICTDEL, new IntegerOutput<>(codec), args(dict).addValues(terms));
    }

    public Command<K, V, List<V>> dictdump(K dict) {
        notNull(dict, "dict");
        return createCommand(CommandType.DICTDUMP, new ValueListOutput<>(codec), args(dict));
    }

    public Command<K, V, Long> sugadd(K key, V string, double score) {
        return sugadd(key, string, score, null);
    }

    public Command<K, V, Long> sugadd(K key, V string, double score, SugaddOptions<V> options) {
        notNull(key, "key");
        notNull(string, "suggestion string");
        RediSearchCommandArgs<K, V> args = args(key);
        args.addValue(string);
        args.add(score);
        if (options != null) {
            if (options.isIncrement()) {
                args.add(CommandKeyword.INCR);
            }
            if (options.getPayload() != null) {
                args.add(CommandKeyword.PAYLOAD);
                args.addValue(options.getPayload());
            }
        }
        return createCommand(CommandType.SUGADD, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, List<Suggestion<V>>> sugget(K key, V prefix) {
        return sugget(key, prefix, null);
    }

    public Command<K, V, List<Suggestion<V>>> sugget(K key, V prefix, SuggetOptions options) {
        notNull(key, "key");
        notNull(prefix, "prefix");
        RediSearchCommandArgs<K, V> args = args(key);
        args.addValue(prefix);
        if (options != null) {
            options.build(args);
        }
        return createCommand(CommandType.SUGGET, suggetOutput(options), args);
    }

    private SuggetOutput<K, V> suggetOutput(SuggetOptions options) {
        if (options == null) {
            return new SuggetOutput<>(codec);
        }
        return new SuggetOutput<>(codec, options.isWithScores(), options.isWithPayloads());
    }

    public Command<K, V, Boolean> sugdel(K key, V string) {
        notNull(key, "key");
        notNull(string, "string");
        return createCommand(CommandType.SUGDEL, new BooleanOutput<>(codec), args(key).addValue(string));
    }

    public Command<K, V, Long> suglen(K key) {
        notNull(key, "key");
        return createCommand(CommandType.SUGLEN, new IntegerOutput<>(codec), args(key));

    }

    public Command<K, V, String> aliasAdd(K name, K index) {
        notNull(name, "name");
        notNull(index, "index");
        return createCommand(CommandType.ALIASADD, new StatusOutput<>(codec), args(name).addKey(index));
    }

    public Command<K, V, String> aliasUpdate(K name, K index) {
        notNull(name, "name");
        notNull(index, "index");
        return createCommand(CommandType.ALIASUPDATE, new StatusOutput<>(codec), args(name).addKey(index));
    }

    public Command<K, V, String> aliasDel(K name) {
        notNull(name, "name");
        return createCommand(CommandType.ALIASDEL, new StatusOutput<>(codec), args(name));
    }

    public Command<K, V, List<K>> list() {
        return new Command<>(CommandType._LIST, new KeyListOutput<>(codec));
    }

}
