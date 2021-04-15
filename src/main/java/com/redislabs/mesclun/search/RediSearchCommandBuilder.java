package com.redislabs.mesclun.search;

import com.redislabs.mesclun.impl.RedisModulesCommandBuilder;
import com.redislabs.mesclun.search.output.*;
import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.CommandType;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;
import java.util.Map;

/**
 * Dedicated pub/sub command builder to build pub/sub commands.
 */
@SuppressWarnings("unchecked")
public class RediSearchCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

    public RediSearchCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected <A, B, T> Command<A, B, T> createCommand(CommandType type, CommandOutput<A, B, T> output, CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    public Command<K, V, String> create(K index, CreateOptions<K, V> options, Field<K, V>... fields) {
        notNull(index, "index");
        LettuceAssert.isTrue(fields.length > 0, "At least one field is required.");
        RediSearchCommandArgs<K, V> args = createArgs(index);
        if (options != null) {
            options.build(args);
        }
        args.add(CommandKeyword.SCHEMA);
        for (Field<K, V> field : fields) {
            field.build(args);
        }
        return createCommand(CommandType.CREATE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> dropIndex(K index, boolean deleteDocs) {
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = createArgs(index);
        if (deleteDocs) {
            args.add(CommandKeyword.DD);
        }
        return createCommand(CommandType.DROPINDEX, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Object>> info(K index) {
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = createArgs(index);
        return createCommand(CommandType.INFO, new NestedMultiOutput<>(codec), args);
    }

    public Command<K, V, String> alter(K index, Field<K,V> field) {
        notNull(index, "index");
        notNull(field, "field");
        RediSearchCommandArgs<K, V> args = createArgs(index);
        args.add(CommandKeyword.SCHEMA);
        args.add(CommandKeyword.ADD);
        field.build(args);
        return createCommand(CommandType.ALTER, new StatusOutput<>(codec), args);
    }

    private RediSearchCommandArgs<K, V> createArgs(K index) {
        return new RediSearchCommandArgs<>(codec).addKey(index);
    }

    public Command<K, V, SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        notNull(index, "index");
        notNull(query, "query");
        RediSearchCommandArgs<K, V> commandArgs = createArgs(index);
        commandArgs.addValue(query);
        if (options != null) {
            options.build(commandArgs);
        }
        return createCommand(CommandType.SEARCH, searchOutput(options), commandArgs);
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
        RediSearchCommandArgs<K, V> args = createArgs(index);
        args.addValue(query);
        if (options != null) {
            options.build(args);
        }
        return createCommand(CommandType.AGGREGATE, new AggregateOutput<>(codec, new AggregateResults<>()), args);
    }

    public Command<K, V, AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options) {
        notNull(index, "index");
        notNull(query, "query");
        RediSearchCommandArgs<K, V> args = createArgs(index);
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


    public Command<K, V, Long> sugadd(K key, V string, double score) {
        return sugadd(key, string, score, null);
    }

    public Command<K, V, Long> sugadd(K key, V string, double score, SugaddOptions<K, V> options) {
        notNull(key, "key");
        notNull(string, "suggestion string");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec);
        args.addKey(key);
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
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec);
        args.addKey(key);
        args.addValue(prefix);
        if (options != null) {
            options.build(args);
        }
        return createCommand(CommandType.SUGGET, suggestOutput(options), args);
    }

    private SuggestOutput<K, V> suggestOutput(SuggetOptions options) {
        if (options == null) {
            return new SuggestOutput<>(codec);
        }
        return new SuggestOutput<>(codec, options.isWithScores(), options.isWithPayloads());
    }

    public Command<K, V, Boolean> sugdel(K key, V string) {
        notNull(key, "key");
        notNull(string, "string");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec).addKey(key).addValue(string);
        return createCommand(CommandType.SUGDEL, new BooleanOutput<>(codec), args);
    }

    public Command<K, V, Long> suglen(K key) {
        notNull(key, "key");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec).addKey(key);
        return createCommand(CommandType.SUGLEN, new IntegerOutput<>(codec), args);

    }

    public Command<K, V, Map<K, V>> get(K index, K docId) {
        notNull(docId, "docId");
        RediSearchCommandArgs<K, V> args = createArgs(index);
        args.addKey(docId);
        return createCommand(CommandType.GET, new MapOutput<>(codec), args);
    }

    public Command<K, V, String> aliasAdd(K name, K index) {
        notNull(name, "name");
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec);
        args.addKey(name);
        args.addKey(index);
        return createCommand(CommandType.ALIASADD, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> aliasUpdate(K name, K index) {
        notNull(name, "name");
        notNull(index, "index");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec);
        args.addKey(name);
        args.addKey(index);
        return createCommand(CommandType.ALIASUPDATE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> aliasDel(K name) {
        notNull(name, "name");
        RediSearchCommandArgs<K, V> args = new RediSearchCommandArgs<>(codec);
        args.addKey(name);
        return createCommand(CommandType.ALIASDEL, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<K>> list() {
        return new Command<>(CommandType._LIST, new KeyListOutput<>(codec));
    }

}
