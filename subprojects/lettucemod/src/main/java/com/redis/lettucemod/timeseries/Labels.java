package com.redis.lettucemod.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class Labels<K> implements CompositeArgument {

	private List<K> names = new ArrayList<>();

	public Labels() {
	}

	public Labels(List<K> names) {
		this.names = names;
	}

	public List<K> getNames() {
		return names;
	}

	public void addName(K name) {
		this.names.add(name);
	}

	public void setNames(List<K> names) {
		this.names = names;
	}

	@SuppressWarnings({ "hiding", "unchecked" })
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		if (names.isEmpty()) {
			args.add(TimeSeriesCommandKeyword.WITHLABELS);
		} else {
			args.add(TimeSeriesCommandKeyword.SELECTED_LABELS);
			names.forEach(l -> args.addKey((K) l));
		}
	}

	@SuppressWarnings("unchecked")
	public static <K> Labels<K> of(K... names) {
		return new Labels<>(Arrays.asList(names));
	}

}
