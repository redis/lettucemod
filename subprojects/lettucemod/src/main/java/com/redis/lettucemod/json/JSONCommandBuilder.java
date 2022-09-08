package com.redis.lettucemod.json;

import java.util.Arrays;
import java.util.List;

import com.redis.lettucemod.RedisModulesCommandBuilder;
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

/**
 * Builder dedicated to RedisJSON commands.
 */
public class JSONCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

	public JSONCommandBuilder(RedisCodec<K, V> codec) {
		super(codec);
	}

	protected <A, B, T> Command<A, B, T> createCommand(JsonCommandType type, CommandOutput<A, B, T> output,
			CommandArgs<A, B> args) {
		return new Command<>(type, output, args);
	}

	protected static void notNullPath(Object path) {
		notNull(path, "Path");
	}

	public Command<K, V, Long> del(K key, String path) {
		CommandArgs<K, V> args = args(key);
		if (path != null) {
			args.add(path);
		}
		return createCommand(JsonCommandType.DEL, new IntegerOutput<>(codec), args);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, V> get(K key, GetOptions options, K... paths) {
		CommandArgs<K, V> args = args(key);
		if (options != null) {
			options.build(args);
		}
		args.addKeys(paths);
		return createCommand(JsonCommandType.GET, new ValueOutput<>(codec), args);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<V>> mget(String path, K... keys) {
		notEmptyKeys(keys);
		notNullPath(path);

		CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
		args.add(path);
		return createCommand(JsonCommandType.MGET, new ValueListOutput<>(codec), args);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, Long> mget(KeyValueStreamingChannel<K, V> channel, String path, K... keys) {
		notEmptyKeys(keys);
		notNullPath(path);
		notNull(channel);

		CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
		args.add(path);
		return createCommand(JsonCommandType.MGET, new KeyValueStreamingOutput<>(codec, channel, Arrays.asList(keys)),
				args);
	}

	public Command<K, V, Long> mget(KeyValueStreamingChannel<K, V> channel, String path, Iterable<K> keys) {
		notNull(keys, "Keys");
		notNullPath(path);
		notNull(channel);

		CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
		args.add(path);
		return createCommand(JsonCommandType.MGET, new KeyValueStreamingOutput<>(codec, channel, keys), args);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<KeyValue<K, V>>> mgetKeyValue(String path, K... keys) {
		notEmptyKeys(keys);
		notNullPath(path);

		CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
		args.add(path);
		return createCommand(JsonCommandType.MGET, new KeyValueListOutput<>(codec, Arrays.asList(keys)), args);
	}

	public Command<K, V, List<KeyValue<K, V>>> mgetKeyValue(String path, Iterable<K> keys) {
		notNull(keys, "Keys");
		notNullPath(path);

		CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
		args.add(path);
		return createCommand(JsonCommandType.MGET, new KeyValueListOutput<>(codec, keys), args);
	}

	public Command<K, V, String> set(K key, String path, V json, SetMode mode) {
		CommandArgs<K, V> args = args(key);
		notNullPath(path);
		args.add(path);
		notNull(json, "JSON");
		args.addValue(json);
		if (mode != null) {
			mode.build(args);
		}
		return createCommand(JsonCommandType.SET, new StatusOutput<>(codec), args);
	}

	public Command<K, V, String> type(K key, String path) {
		CommandArgs<K, V> args = args(key);
		if (path != null) {
			args.add(path);
		}
		return createCommand(JsonCommandType.TYPE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, V> numIncrBy(K key, String path, double number) {
		CommandArgs<K, V> args = args(key);
		notNullPath(path);
		args.add(path);
		args.add(number);
		return createCommand(JsonCommandType.NUMINCRBY, new ValueOutput<>(codec), args);
	}

	public Command<K, V, V> numMultBy(K key, String path, double number) {
		CommandArgs<K, V> args = args(key);
		notNullPath(path);
		args.add(path);
		args.add(number);
		return createCommand(JsonCommandType.NUMMULTBY, new ValueOutput<>(codec), args);
	}

	public Command<K, V, Long> strAppend(K key, String path, V json) {
		CommandArgs<K, V> args = args(key);
		if (path != null) {
			args.add(path);
		}
		args.addValue(json);
		return createCommand(JsonCommandType.STRAPPEND, new IntegerOutput<>(codec), args);
	}

	public Command<K, V, Long> strLen(K key, String path) {
		CommandArgs<K, V> args = args(key);
		if (path != null) {
			args.add(path);
		}
		return createCommand(JsonCommandType.STRLEN, new IntegerOutput<>(codec), args);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, Long> arrAppend(K key, String path, V... jsons) {
		CommandArgs<K, V> args = args(key);
		notNullPath(path);
		args.add(path);
		notEmpty(jsons, "JSONs");
		args.addValues(jsons);
		return createCommand(JsonCommandType.ARRAPPEND, new IntegerOutput<>(codec), args);
	}

	public Command<K, V, Long> arrIndex(K key, String path, V jsonScalar, Slice slice) {
		CommandArgs<K, V> args = args(key);
		notNullPath(path);
		args.add(path);
		notNull(jsonScalar, "JSON scalar");
		args.addValue(jsonScalar);
		if (slice != null) {
			slice.build(args);
		}
		return createCommand(JsonCommandType.ARRINDEX, new IntegerOutput<>(codec), args);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, Long> arrInsert(K key, String path, long index, V... jsons) {
		CommandArgs<K, V> args = args(key);
		notNullPath(path);
		args.add(path);
		args.add(index);
		notEmpty(jsons, "JSONs");
		args.addValues(jsons);
		return createCommand(JsonCommandType.ARRINSERT, new IntegerOutput<>(codec), args);
	}

	public Command<K, V, Long> arrLen(K key, String path) {
		CommandArgs<K, V> args = args(key);
		if (path != null) {
			args.add(path);
		}
		return createCommand(JsonCommandType.ARRLEN, new IntegerOutput<>(codec), args);
	}

	public Command<K, V, V> arrPop(K key, ArrpopOptions<K> options) {
		CommandArgs<K, V> args = args(key);
		if (options != null) {
			options.build(args);
		}
		return createCommand(JsonCommandType.ARRPOP, new ValueOutput<>(codec), args);
	}

	public Command<K, V, Long> arrTrim(K key, String path, long start, long stop) {
		CommandArgs<K, V> args = args(key);
		notNullPath(path);
		args.add(path);
		args.add(start);
		args.add(stop);
		return createCommand(JsonCommandType.ARRTRIM, new IntegerOutput<>(codec), args);
	}

	public Command<K, V, List<K>> objKeys(K key, String path) {
		CommandArgs<K, V> args = args(key);
		if (path != null) {
			args.add(path);
		}
		return createCommand(JsonCommandType.OBJKEYS, new KeyListOutput<>(codec), args);
	}

	public Command<K, V, Long> objLen(K key, String path) {
		CommandArgs<K, V> args = args(key);
		if (path != null) {
			args.add(path);
		}
		return createCommand(JsonCommandType.OBJLEN, new IntegerOutput<>(codec), args);
	}

}
