package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public enum Order {

	ASC(SearchCommandKeyword.ASC), DESC(SearchCommandKeyword.DESC);

	private final SearchCommandKeyword keyword;

	private Order(SearchCommandKeyword keyword) {
		this.keyword = keyword;
	}

	public SearchCommandKeyword getKeyword() {
		return keyword;
	}

}
