package com.redis.lettucemod.timeseries;

import java.util.List;

import com.redis.lettucemod.RedisModulesCommandBuilder;
import com.redis.lettucemod.output.GetOutput;
import com.redis.lettucemod.output.RangeOutput;
import com.redis.lettucemod.output.SampleListOutput;
import com.redis.lettucemod.output.SampleOutput;
import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;
import com.redis.lettucemod.protocol.TimeSeriesCommandType;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerListOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

/**
 * Builder for RedisTimeSeries commands.
 */
public class RedisTimeSeriesCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

	private static final String AUTO_TIMESTAMP = "*";

	public RedisTimeSeriesCommandBuilder(RedisCodec<K, V> codec) {
		super(codec);
	}

	protected <A, B, T> Command<A, B, T> createCommand(TimeSeriesCommandType type, CommandOutput<A, B, T> output,
			CommandArgs<A, B> args) {
		return new Command<>(type, output, args);
	}

	public Command<K, V, String> create(K key, CreateOptions<K, V> options) {
		CommandArgs<K, V> args = args(key);
		if (options != null) {
			options.build(args);
		}
		return createCommand(TimeSeriesCommandType.CREATE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, String> alter(K key, CreateOptions<K, V> options) {
		CommandArgs<K, V> args = args(key);
		if (options != null) {
			options.build(args);
		}
		return createCommand(TimeSeriesCommandType.ALTER, new StatusOutput<>(codec), args);
	}

	public Command<K, V, Long> add(K key, Sample sample) {
		return add(key, sample, null);
	}

	public Command<K, V, Long> add(K key, Sample sample, CreateOptions<K, V> options) {
		notNull(sample, "Sample");
		return doAdd(key, sample.getTimestamp(), sample.getValue(), options);
	}

	public Command<K, V, Long> add(K key, long timestamp, double value) {
		return doAdd(key, timestamp, value, null);
	}

	public Command<K, V, Long> add(K key, long timestamp, double value, CreateOptions<K, V> options) {
		return doAdd(key, timestamp, value, options);
	}

	public Command<K, V, Long> addAutoTimestamp(K key, double value) {
		return addAutoTimestamp(key, value, null);
	}

	public Command<K, V, Long> addAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return doAdd(key, Sample.AUTO_TIMESTAMP, value, options);
	}

	private Command<K, V, Long> doAdd(K key, long timestamp, double value, CreateOptions<K, V> options) {
		CommandArgs<K, V> args = args(key);
		add(args, timestamp, value);
		if (options != null) {
			options.build(args);
		}
		return createCommand(TimeSeriesCommandType.ADD, new IntegerOutput<>(codec), args);
	}

	private void add(CommandArgs<K, V> args, long timestamp, double value) {
		if (timestamp == Sample.AUTO_TIMESTAMP) {
			args.add(AUTO_TIMESTAMP);
		} else {
			args.add(timestamp);
		}
		args.add(value);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<Long>> madd(KeySample<K>... samples) {
		notEmpty(samples, "Samples");
		CommandArgs<K, V> args = new CommandArgs<>(codec);
		for (KeySample<K> sample : samples) {
			args.addKey(sample.getKey());
			add(args, sample.getTimestamp(), sample.getValue());
		}
		return createCommand(TimeSeriesCommandType.MADD, new IntegerListOutput<>(codec), args);
	}

	public Command<K, V, Long> incrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return deincrby(TimeSeriesCommandType.INCRBY, key, value, null, true, options);
	}

	public Command<K, V, Long> decrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return deincrby(TimeSeriesCommandType.DECRBY, key, value, null, true, options);
	}

	public Command<K, V, Long> incrby(K key, double value, Long timestamp, CreateOptions<K, V> options) {
		return deincrby(TimeSeriesCommandType.INCRBY, key, value, timestamp, false, options);
	}

	public Command<K, V, Long> decrby(K key, double value, Long timestamp, CreateOptions<K, V> options) {
		return deincrby(TimeSeriesCommandType.DECRBY, key, value, timestamp, false, options);
	}

	private Command<K, V, Long> deincrby(TimeSeriesCommandType commandType, K key, double value, Long timestamp,
			boolean autoTimestamp, CreateOptions<K, V> options) {
		CommandArgs<K, V> args = args(key);
		args.add(value);
		if (autoTimestamp) {
			args.add(TimeSeriesCommandKeyword.TIMESTAMP);
			args.add(AUTO_TIMESTAMP);
		} else {
			if (timestamp != null) {
				args.add(TimeSeriesCommandKeyword.TIMESTAMP);
				args.add(timestamp);
			}
		}
		if (options != null) {
			options.build(args);
		}
		return createCommand(commandType, new IntegerOutput<>(codec), args);
	}

	public Command<K, V, String> createRule(K sourceKey, K destKey, Aggregation aggregation) {
		notNull(sourceKey, "Source key");
		notNull(destKey, "Destination key");
		notNull(aggregation, "Aggregation");
		CommandArgs<K, V> args = args(sourceKey);
		args.addKey(destKey);
		aggregation.build(args);
		return createCommand(TimeSeriesCommandType.CREATERULE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, String> deleteRule(K sourceKey, K destKey) {
		notNull(sourceKey, "Source key");
		notNull(destKey, "Destination key");
		CommandArgs<K, V> args = args(sourceKey);
		args.addKey(destKey);
		return createCommand(TimeSeriesCommandType.DELETERULE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, List<Sample>> range(K key, RangeOptions options) {
		return range(TimeSeriesCommandType.RANGE, key, options);
	}

	public Command<K, V, List<Sample>> revrange(K key, RangeOptions options) {
		return range(TimeSeriesCommandType.REVRANGE, key, options);
	}

	private Command<K, V, List<Sample>> range(TimeSeriesCommandType commandType, K key, RangeOptions options) {
		notNull(options, "Options");
		CommandArgs<K, V> args = args(key);
		options.build(args);
		return createCommand(commandType, new SampleListOutput<>(codec), args);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<RangeResult<K, V>>> mrange(RangeOptions options, V... filters) {
		return mrange(false, false, options, filters);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<RangeResult<K, V>>> mrangeWithLabels(RangeOptions options, V... filters) {
		return mrange(false, true, options, filters);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<RangeResult<K, V>>> mrevrange(RangeOptions options, V... filters) {
		return mrange(true, false, options, filters);
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<RangeResult<K, V>>> mrevrangeWithLabels(RangeOptions options, V... filters) {
		return mrange(true, true, options, filters);
	}

	@SuppressWarnings("unchecked")
	private Command<K, V, List<RangeResult<K, V>>> mrange(boolean reverse, boolean withLabels, RangeOptions options,
			V... filters) {
		notNull(options, "Options");
		notEmpty(filters, "Filters");
		CommandArgs<K, V> args = new CommandArgs<>(codec);
		options.build(args);
		if (withLabels) {
			args.add(TimeSeriesCommandKeyword.WITHLABELS);
		}
		args.add(TimeSeriesCommandKeyword.FILTER);
		args.addValues(filters);
		return createCommand(reverse ? TimeSeriesCommandType.MREVRANGE : TimeSeriesCommandType.MRANGE,
				new RangeOutput<>(codec), args);
	}

	public Command<K, V, Sample> get(K key) {
		return createCommand(TimeSeriesCommandType.GET, new SampleOutput<>(codec), args(key));
	}

	@SuppressWarnings("unchecked")
	public Command<K, V, List<GetResult<K, V>>> mget(boolean withLabels, V... filters) {
		CommandArgs<K, V> args = new CommandArgs<>(codec);
		if (withLabels) {
			args.add(TimeSeriesCommandKeyword.WITHLABELS);
		}
		args.add(TimeSeriesCommandKeyword.FILTER);
		args.addValues(filters);
		return createCommand(TimeSeriesCommandType.MGET, new GetOutput<>(codec), args);
	}

	public Command<K, V, List<Object>> info(K key, boolean debug) {
		notNullKey(key);
		CommandArgs<K, V> args = args(key);
		if (debug) {
			args.add(TimeSeriesCommandKeyword.DEBUG);
		}
		return createCommand(TimeSeriesCommandType.INFO, new NestedMultiOutput<>(codec), args);
	}
}
