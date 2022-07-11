package com.redis.lettucemod.search;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.ByteBuffer;

/**
 * Command args for RediSearch connections. This implementation hides the first
 * key as RediSearch keys are not keys from the key-space.
 *
 * @author Julien Ruaux
 */
public class SearchCommandArgs<K, V> extends CommandArgs<K, V> {

	private static final String PREFIX = "@";

	/**
	 * @param codec Codec used to encode/decode keys and values, must not be
	 *              {@literal null}.
	 */
	public SearchCommandArgs(RedisCodec<K, V> codec) {
		super(codec);
	}

	/**
	 * @return always {@literal null}.
	 */
	@Override
	public ByteBuffer getFirstEncodedKey() {
		return null;
	}

	public SearchCommandArgs<K, V> addProperty(String property) {
		add(property(property));
		return this;
	}

	public static String property(String name) {
		return PREFIX + name;
	}

	@Override
	public SearchCommandArgs<K, V> add(String s) {
		super.add(s);
		return this;
	}

	@Override
	public SearchCommandArgs<K, V> add(CommandKeyword keyword) {
		return (SearchCommandArgs<K, V>) super.add(keyword);
	}

	@Override
	public SearchCommandArgs<K, V> add(long n) {
		return (SearchCommandArgs<K, V>) super.add(n);
	}

	@Override
	public SearchCommandArgs<K, V> addKey(K key) {
		super.addKey(key);
		return this;
	}

	@Override
	public SearchCommandArgs<K, V> addValue(V value) {
		super.addValue(value);
		return this;
	}

	@Override
	public SearchCommandArgs<K, V> add(double n) {
		super.add(n);
		return this;
	}
	
	@Override
	public SearchCommandArgs<K, V> add(ProtocolKeyword keyword) {
		return (SearchCommandArgs<K, V>) super.add(keyword);
	}
}