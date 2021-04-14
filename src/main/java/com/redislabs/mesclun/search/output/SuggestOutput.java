package com.redislabs.mesclun.search.output;

import com.redislabs.mesclun.search.Suggestion;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SuggestOutput<K, V> extends CommandOutput<K, V, List<Suggestion<V>>> {

	private final boolean withScores;
	private final boolean withPayloads;
	private Suggestion<V> current;
	private boolean payloadSet = false;
	private boolean scoreSet = false;

	public SuggestOutput(RedisCodec<K, V> codec) {
		this(codec, false, false);
	}

	public SuggestOutput(RedisCodec<K, V> codec, boolean withScores, boolean withPayloads) {
		super(codec, new ArrayList<>());
		this.withScores = withScores;
		this.withPayloads = withPayloads;
	}

	@Override
	public void set(ByteBuffer bytes) {
		if (current == null) {
			current = new Suggestion<>();
			payloadSet = false;
			scoreSet = false;
			if (bytes != null) {
				current.setString(codec.decodeValue(bytes));
			}
			output.add(current);
			if (!withScores && !withPayloads) {
				current = null;
			}
		} else {
			if (withPayloads && !payloadSet) {
				if (bytes != null) {
					current.setPayload(codec.decodeValue(bytes));
				}
				payloadSet = true;
				current = null;
			}
		}
	}

	@Override
	public void set(double number) {
		if (withScores && !scoreSet) {
			current.setScore(number);
			scoreSet = true;
			if (!withPayloads) {
				current = null;
			}
		}
	}

}
