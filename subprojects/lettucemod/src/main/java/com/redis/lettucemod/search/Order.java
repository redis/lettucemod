package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public enum Order {

	ASC(SearchCommandKeyword.ASC), DESC(SearchCommandKeyword.DESC);

	private final SearchCommandKeyword keyword;

	private Order(SearchCommandKeyword keyword) {
		this.keyword = keyword;
	}

	@SuppressWarnings("rawtypes")
	public void build(SearchCommandArgs args) {
		args.add(keyword);
	}

}
