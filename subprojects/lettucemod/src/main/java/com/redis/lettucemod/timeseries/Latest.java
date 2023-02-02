package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class Latest implements CompositeArgument {

	private final boolean enabled;

	public Latest() {
		this(false);
	}

	public boolean isEnabled() {
		return enabled;
	}

	public Latest(boolean enabled) {
		this.enabled = enabled;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		if (enabled) {
			args.add(TimeSeriesCommandKeyword.LATEST);
		}
	}

	public static Latest of(boolean latest) {
		return new Latest(latest);
	}

}
