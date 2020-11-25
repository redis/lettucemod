package com.redislabs.mesclun;

import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import com.redislabs.mesclun.timeseries.protocol.CommandKeyword;
import com.redislabs.mesclun.timeseries.protocol.CommandType;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.BaseRedisCommandBuilder;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

/**
 * Dedicated pub/sub command builder to build pub/sub commands.
 */
public class RedisTimeSeriesCommandBuilder<K, V> extends BaseRedisCommandBuilder<K, V> {

	static final String MUST_NOT_BE_NULL = "must not be null";
	static final String MUST_NOT_BE_EMPTY = "must not be empty";

	public RedisTimeSeriesCommandBuilder(RedisCodec<K, V> codec) {
		super(codec);
	}

	private void assertNotNull(Object arg, String name) {
		LettuceAssert.notNull(arg, name + " " + MUST_NOT_BE_NULL);
	}

	protected <A, B, T> Command<A, B, T> createCommand(CommandType type, CommandOutput<A, B, T> output,
			CommandArgs<A, B> args) {
		return new Command<>(type, output, args);
	}

	public Command<K, V, String> create(K key, CreateOptions options, Label<K, V>[] labels) {
		CommandArgs<K, V> args = args(key);
		addOptions(args, options, labels);
		return createCommand(CommandType.CREATE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>[] labels) {
		CommandArgs<K, V> args = args(key);
		args.add(timestamp);
		args.add(value);
		addOptions(args, options, labels);
		return createCommand(CommandType.ADD, new IntegerOutput<>(codec), args);
	}

	private void addOptions(CommandArgs<K, V> args, CreateOptions options, Label<K, V>[] labels) {
		if (options != null) {
			options.build(args);
		}
		if (labels != null && labels.length > 0) {
			args.add(CommandKeyword.LABELS);
			for (Label<K, V> label : labels) {
				args.addKey(label.getLabel());
				args.addValue(label.getValue());
			}
		}
	}

	private CommandArgs<K, V> args(K key) {
		assertNotNull(key, "key");
		return new CommandArgs<>(codec).addKey(key);
	}

}
