package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

public enum DuplicatePolicy {
	BLOCK(TimeSeriesCommandKeyword.BLOCK), FIRST(TimeSeriesCommandKeyword.FIRST), LAST(TimeSeriesCommandKeyword.LAST),
	MIN(TimeSeriesCommandKeyword.MIN), MAX(TimeSeriesCommandKeyword.MAX), SUM(TimeSeriesCommandKeyword.SUM);

	private final TimeSeriesCommandKeyword keyword;

	DuplicatePolicy(TimeSeriesCommandKeyword keyword) {
		this.keyword = keyword;
	}

	public TimeSeriesCommandKeyword getKeyword() {
		return keyword;
	}
}
