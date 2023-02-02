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
@SuppressWarnings("unchecked")
public class TimeSeriesCommandBuilder<K, V> extends RedisModulesCommandBuilder<K, V> {

	private static final String AUTO_TIMESTAMP = "*";

	public TimeSeriesCommandBuilder(RedisCodec<K, V> codec) {
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

	public Command<K, V, String> alter(K key, AlterOptions<K, V> options) {
		CommandArgs<K, V> args = args(key);
		if (options != null) {
			options.build(args);
		}
		return createCommand(TimeSeriesCommandType.ALTER, new StatusOutput<>(codec), args);
	}

	public Command<K, V, Long> add(K key, Sample sample) {
		return add(key, sample, null);
	}

	public Command<K, V, Long> add(K key, Sample sample, AddOptions<K, V> options) {
		notNull(sample, "Sample");
		CommandArgs<K, V> args = args(key);
		add(args, sample.getTimestamp(), sample.getValue());
		if (options != null) {
			options.build(args);
		}
		return createCommand(TimeSeriesCommandType.ADD, new IntegerOutput<>(codec), args);
	}

	private void add(CommandArgs<K, V> args, long timestamp, double value) {
		addTimestamp(args, timestamp);
		args.add(value);
	}

	public static <K, V> void addTimestamp(CommandArgs<K, V> args, long timestamp) {
		if (timestamp == Sample.AUTO_TIMESTAMP) {
			args.add(AUTO_TIMESTAMP);
		} else {
			args.add(timestamp);
		}
	}

	public Command<K, V, List<Long>> madd(KeySample<K>... samples) {
		notEmpty(samples, "Samples");
		CommandArgs<K, V> args = new CommandArgs<>(codec);
		for (KeySample<K> sample : samples) {
			args.addKey(sample.getKey());
			add(args, sample.getTimestamp(), sample.getValue());
		}
		return createCommand(TimeSeriesCommandType.MADD, new IntegerListOutput<>(codec), args);
	}

	public Command<K, V, Long> incrby(K key, double value, IncrbyOptions<K, V> options) {
		return incrby(TimeSeriesCommandType.INCRBY, key, value, options);
	}

	public Command<K, V, Long> decrby(K key, double value, IncrbyOptions<K, V> options) {
		return incrby(TimeSeriesCommandType.DECRBY, key, value, options);
	}

	private Command<K, V, Long> incrby(TimeSeriesCommandType commandType, K key, double value,
			IncrbyOptions<K, V> options) {
		CommandArgs<K, V> args = args(key);
		args.add(value);
		if (options != null) {
			options.build(args);
		}
		return createCommand(commandType, new IntegerOutput<>(codec), args);
	}

	public Command<K, V, String> createRule(K sourceKey, K destKey, CreateRuleOptions options) {
		notNull(sourceKey, "Source key");
		notNull(destKey, "Destination key");
		notNull(options, "Options");
		CommandArgs<K, V> args = args(sourceKey);
		args.addKey(destKey);
		options.build(args);
		return createCommand(TimeSeriesCommandType.CREATERULE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, String> deleteRule(K sourceKey, K destKey) {
		notNull(sourceKey, "Source key");
		notNull(destKey, "Destination key");
		CommandArgs<K, V> args = args(sourceKey);
		args.addKey(destKey);
		return createCommand(TimeSeriesCommandType.DELETERULE, new StatusOutput<>(codec), args);
	}

	public Command<K, V, List<Sample>> range(K key, TimeRange range) {
		return range(key, range, null);
	}

	public Command<K, V, List<Sample>> range(K key, TimeRange range, RangeOptions options) {
		return range(TimeSeriesCommandType.RANGE, key, range, options);
	}

	public Command<K, V, List<Sample>> revrange(K key, TimeRange range) {
		return revrange(key, range, null);
	}

	public Command<K, V, List<Sample>> revrange(K key, TimeRange range, RangeOptions options) {
		return range(TimeSeriesCommandType.REVRANGE, key, range, options);
	}

	private Command<K, V, List<Sample>> range(TimeSeriesCommandType commandType, K key, TimeRange range,
			RangeOptions options) {
		notNull(range, "Time range");
		CommandArgs<K, V> args = args(key);
		range.build(args);
		if (options != null) {
			options.build(args);
		}
		return createCommand(commandType, new SampleListOutput<>(codec), args);
	}

	public Command<K, V, List<RangeResult<K, V>>> mrange(TimeRange range) {
		return mrange(range, null);
	}

	public Command<K, V, List<RangeResult<K, V>>> mrange(TimeRange range, MRangeOptions<K, V> options) {
		return mrange(RangeDirection.FORWARD, range, options);
	}

	public Command<K, V, List<RangeResult<K, V>>> mrevrange(TimeRange range) {
		return mrevrange(range, null);
	}

	public Command<K, V, List<RangeResult<K, V>>> mrevrange(TimeRange range, MRangeOptions<K, V> options) {
		return mrange(RangeDirection.REVERSE, range, options);
	}

	private enum RangeDirection {
		FORWARD, REVERSE
	}

	private Command<K, V, List<RangeResult<K, V>>> mrange(RangeDirection direction, TimeRange range,
			MRangeOptions<K, V> options) {
		notNull(range, "Time range");
		CommandArgs<K, V> args = new CommandArgs<>(codec);
		range.build(args);
		if (options != null) {
			options.build(args);
		}
		return createCommand(
				direction == RangeDirection.REVERSE ? TimeSeriesCommandType.MREVRANGE : TimeSeriesCommandType.MRANGE,
				new RangeOutput<>(codec), args);
	}

	public Command<K, V, Sample> get(K key) {
		return createCommand(TimeSeriesCommandType.GET, new SampleOutput<>(codec), args(key));
	}

	public Command<K, V, List<GetResult<K, V>>> mgetWithLabels(V... filters) {
		return mget(MGetOptions.<K, V>filters(filters).withLabels().build());
	}

	public Command<K, V, List<GetResult<K, V>>> mget(V... filters) {
		return mget(MGetOptions.<K, V>filters(filters).build());
	}

	public Command<K, V, List<GetResult<K, V>>> mget(MGetOptions<K, V> options) {
		CommandArgs<K, V> args = new CommandArgs<>(codec);
		options.build(args);
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
