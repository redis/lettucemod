package com.redis.lettucemod;

import com.redis.lettucemod.api.JsonGetOptions;
import com.redis.lettucemod.protocol.JsonCommandKeyword;
import com.redis.lettucemod.protocol.JsonCommandType;
import io.lettuce.core.KeyValue;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.KeyListOutput;
import io.lettuce.core.output.KeyValueListOutput;
import io.lettuce.core.output.KeyValueStreamingChannel;
import io.lettuce.core.output.KeyValueStreamingOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueListOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.Arrays;
import java.util.List;

/**
 * Builder dedicated to RedisJSON commands.
 */
public class RedisJSONCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

    public RedisJSONCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected <A, B, T> Command<A, B, T> createCommand(JsonCommandType type, CommandOutput<A, B, T> output, CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    protected static void notNullPath(Object path) {
        notNull(path, "Path");
    }

    public Command<K, V, Long> del(K key, K path) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
        }
        return createCommand(JsonCommandType.DEL, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, V> get(K key, JsonGetOptions<K, V> options, K... paths) {
        CommandArgs<K, V> args = args(key);
        if (options != null) {
            options.build(args);
        }
        args.addKeys(paths);
        return createCommand(JsonCommandType.GET, new ValueOutput<>(codec), args);
    }

    public Command<K, V, List<V>> mget(K path, K... keys) {
        notEmptyKeys(keys);
        notNullPath(path);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        args.addKey(path);
        return createCommand(JsonCommandType.MGET, new ValueListOutput<>(codec), args);
    }

    public Command<K, V, Long> mget(KeyValueStreamingChannel<K, V> channel, K path, K... keys) {
        notEmptyKeys(keys);
        notNullPath(path);
        notNull(channel);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        args.addKey(path);
        return createCommand(JsonCommandType.MGET, new KeyValueStreamingOutput<>(codec, channel, Arrays.asList(keys)), args);
    }

    public Command<K, V, Long> mget(KeyValueStreamingChannel<K, V> channel, K path, Iterable<K> keys) {
        notNull(keys, "Keys");
        notNullPath(path);
        notNull(channel);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        args.addKey(path);
        return createCommand(JsonCommandType.MGET, new KeyValueStreamingOutput<>(codec, channel, keys), args);
    }

    public Command<K, V, List<KeyValue<K, V>>> mgetKeyValue(K path, K... keys) {
        notEmptyKeys(keys);
        notNullPath(path);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        args.addKey(path);
        return createCommand(JsonCommandType.MGET, new KeyValueListOutput<>(codec, Arrays.asList(keys)), args);
    }

    public Command<K, V, List<KeyValue<K, V>>> mgetKeyValue(K path, Iterable<K> keys) {
        notNull(keys, "Keys");
        notNullPath(path);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        args.addKey(path);
        return createCommand(JsonCommandType.MGET, new KeyValueListOutput<>(codec, keys), args);
    }

    public enum SetMode {
        NX, XX
    }

    public Command<K, V, String> set(K key, K path, V json, SetMode mode) {
        CommandArgs<K, V> args = args(key);
        notNullPath(path);
        args.addKey(path);
        notNull(json, "JSON");
        args.addValue(json);
        if (mode != null) {
            args.add(mode == SetMode.NX ? JsonCommandKeyword.NX : JsonCommandKeyword.XX);
        }
        return createCommand(JsonCommandType.SET, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> type(K key, K path) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
        }
        return createCommand(JsonCommandType.TYPE, new StatusOutput<>(codec), args);
    }


    public Command<K, V, V> numIncrBy(K key, K path, double number) {
        CommandArgs<K, V> args = args(key);
        notNullPath(path);
        args.addKey(path);
        args.add(number);
        return createCommand(JsonCommandType.NUMINCRBY, new ValueOutput<>(codec), args);
    }

    public Command<K, V, V> numMultBy(K key, K path, double number) {
        CommandArgs<K, V> args = args(key);
        notNullPath(path);
        args.addKey(path);
        args.add(number);
        return createCommand(JsonCommandType.NUMMULTBY, new ValueOutput<>(codec), args);
    }

    public Command<K, V, Long> strAppend(K key, K path, V json) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
        }
        args.addValue(json);
        return createCommand(JsonCommandType.STRAPPEND, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> strLen(K key, K path) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
        }
        return createCommand(JsonCommandType.STRLEN, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> arrAppend(K key, K path, V... jsons) {
        CommandArgs<K, V> args = args(key);
        notNullPath(path);
        args.addKey(path);
        notEmpty(jsons, "JSONs");
        args.addValues(jsons);
        return createCommand(JsonCommandType.ARRAPPEND, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> arrIndex(K key, K path, V jsonScalar, Long start, Long stop) {
        CommandArgs<K, V> args = args(key);
        notNullPath(path);
        args.addKey(path);
        notNull(jsonScalar, "JSON scalar");
        args.addValue(jsonScalar);
        if (start != null) {
            args.add(start);
            if (stop != null) {
                args.add(stop);
            }
        }
        return createCommand(JsonCommandType.ARRINDEX, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> arrInsert(K key, K path, long index, V... jsons) {
        CommandArgs<K, V> args = args(key);
        notNullPath(path);
        args.addKey(path);
        args.add(index);
        notEmpty(jsons, "JSONs");
        args.addValues(jsons);
        return createCommand(JsonCommandType.ARRINSERT, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> arrLen(K key, K path) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
        }
        return createCommand(JsonCommandType.ARRLEN, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, V> arrPop(K key, K path, Long index) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
            if (index != null) {
                args.add(index);
            }
        }
        return createCommand(JsonCommandType.ARRPOP, new ValueOutput<>(codec), args);
    }

    public Command<K, V, Long> arrTrim(K key, K path, long start, long stop) {
        CommandArgs<K, V> args = args(key);
        notNullPath(path);
        args.addKey(path);
        args.add(start);
        args.add(stop);
        return createCommand(JsonCommandType.ARRTRIM, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, List<K>> objKeys(K key, K path) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
        }
        return createCommand(JsonCommandType.OBJKEYS, new KeyListOutput<>(codec), args);
    }

    public Command<K, V, Long> objLen(K key, K path) {
        CommandArgs<K, V> args = args(key);
        if (path != null) {
            args.addKey(path);
        }
        return createCommand(JsonCommandType.OBJLEN, new IntegerOutput<>(codec), args);
    }

}
