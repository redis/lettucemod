package com.redis.lettucemod.api.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Cursor {

	private Long count;
	private Long maxIdle;

	public <K, V> void build(SearchCommandArgs<K, V> args) {
		if (count != null) {
			args.add(SearchCommandKeyword.COUNT);
			args.add(count);
		}
		if (maxIdle != null) {
			args.add(SearchCommandKeyword.MAXIDLE);
			args.add(maxIdle);
		}
	}

}
