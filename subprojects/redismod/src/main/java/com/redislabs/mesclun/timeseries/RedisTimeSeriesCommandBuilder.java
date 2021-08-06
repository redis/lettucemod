package com.redislabs.mesclun.timeseries;

import com.redislabs.mesclun.RedisModulesCommandBuilder;
import com.redislabs.mesclun.timeseries.protocol.CommandKeyword;
import com.redislabs.mesclun.timeseries.protocol.CommandType;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

/**
 * Dedicated builder to build RedisTimeSeries commands.
 */
public class RedisTimeSeriesCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

	public RedisTimeSeriesCommandBuilder(RedisCodec<K, V> codec) {
		super(codec);
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
		notNull(key, "key");
		return new CommandArgs<>(codec).addKey(key);
	}

	public Command<K, V, String> createRule(K sourceKey, K destKey, Aggregation aggregation, long timeBucket) {
		notNull(sourceKey, "Source key");
		notNull(destKey, "Destination key");
		notNull(aggregation, "Aggregation");
		CommandArgs<K, V> args = args(sourceKey);
		args.addKey(destKey);
		args.add(CommandKeyword.AGGREGATION);
		args.add(aggregation.getName());
		args.add(timeBucket);
		return createCommand(CommandType.CREATERULE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, String> deleteRule(K sourceKey, K destKey) {
		notNull(sourceKey, "Source key");
		notNull(destKey, "Destination key");
		CommandArgs<K, V> args = args(sourceKey);
		args.addKey(destKey);
		return createCommand(CommandType.DELETERULE, new StatusOutput<>(codec), args);
	}

}
