package com.redis.lettucemod.output;

import com.redis.lettucemod.cms.CmsInfo;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class CmsInfoOutput<K, V> extends CommandOutput<K, V, CmsInfo> {

	private String field;

	public CmsInfoOutput(RedisCodec<K, V> codec) {
		super(codec, new CmsInfo());
	}

	@Override
	public void set(ByteBuffer buffer) {
		field = decodeAscii(buffer);
	}

	@Override
	public void set(long integer) {
		switch (field) {
		case "width":
			output.setWidth(integer);
			break;
		case "depth":
			output.setDepth(integer);
			break;
		case "count":
			output.setCount(integer);
			break;
		default:
			break;
		}
	}
}
