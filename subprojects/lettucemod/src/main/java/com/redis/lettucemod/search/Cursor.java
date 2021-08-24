package com.redis.lettucemod.search;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;
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

	public <K, V> void build(RediSearchCommandArgs<K, V> args) {
		if (count != null) {
			args.add(CommandKeyword.COUNT);
			args.add(count);
		}
		if (maxIdle != null) {
			args.add(CommandKeyword.MAXIDLE);
			args.add(maxIdle);
		}
	}

}
