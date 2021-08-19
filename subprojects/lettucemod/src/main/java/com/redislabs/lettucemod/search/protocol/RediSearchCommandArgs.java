package com.redislabs.lettucemod.search.protocol;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.CommandArgs;

import java.nio.ByteBuffer;

/**
 *
 * Command args for RediSearch connections. This implementation hides the first
 * key as RediSearch keys are not keys from the key-space.
 *
 * @author Julien Ruaux
 */
public class RediSearchCommandArgs<K, V> extends CommandArgs<K, V> {

	private static final String PREFIX = "@";

	/**
	 * @param codec Codec used to encode/decode keys and values, must not be
	 *              {@literal null}.
	 */
	public RediSearchCommandArgs(RedisCodec<K, V> codec) {
		super(codec);
	}

	/**
	 *
	 * @return always {@literal null}.
	 */
	@Override
	public ByteBuffer getFirstEncodedKey() {
		return null;
	}

	@SuppressWarnings("UnusedReturnValue")
	public RediSearchCommandArgs<K, V> addProperty(String property) {
		add(property(property));
		return this;
	}

	public static String property(String name) {
		return PREFIX + name;
	}

	@Override
	public RediSearchCommandArgs<K, V> add(String s) {
		super.add(s);
		return this;
	}

	@Override
	public RediSearchCommandArgs<K, V> addKey(K key) {
		super.addKey(key);
		return this;
	}

	@Override
	public RediSearchCommandArgs<K, V> addValue(V value) {
		super.addValue(value);
		return this;
	}

	@Override
	public RediSearchCommandArgs<K, V> add(double n) {
		super.add(n);
		return this;
	}
}
