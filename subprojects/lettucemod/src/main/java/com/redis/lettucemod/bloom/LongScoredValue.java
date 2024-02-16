package com.redis.lettucemod.bloom;

import java.util.Optional;
import java.util.function.Function;

import io.lettuce.core.Value;
import io.lettuce.core.internal.LettuceAssert;

/**
 * A long scored-value extension to {@link Value}.
 *
 * @param <V> Value type.
 * @author Will Glozer
 * @author Mark Paluch
 * @author Julien Ruaux
 */
public class LongScoredValue<V> extends Value<V> {

	private static final long serialVersionUID = 1L;

	private static final LongScoredValue<Object> EMPTY = new LongScoredValue<>(0, null);

	private final long score;

	/**
	 * Serializable constructor.
	 */
	protected LongScoredValue() {
		super(null);
		this.score = 0;
	}

	private LongScoredValue(long score, V value) {
		super(value);
		this.score = score;
	}

	/**
	 * Creates a {@link Value} from a {@code key} and an {@link Optional}. The
	 * resulting value contains the value from the {@link Optional} if a value is
	 * present. Value is empty if the {@link Optional} is empty.
	 *
	 * @param score    the score.
	 * @param optional the optional. May be empty but never {@code null}.
	 * @return the {@link Value}.
	 */
	public static <T extends V, V> LongScoredValue<V> from(long score, Optional<T> optional) {

		LettuceAssert.notNull(optional, "Optional must not be null");

		if (optional.isPresent()) {
			return new LongScoredValue<>(score, optional.get());
		}

		return empty();
	}

	/**
	 * Creates a {@link Value} from a {@code score} and {@code value}. The resulting
	 * value contains the value if the {@code value} is not null.
	 *
	 * @param score the score.
	 * @param value the value. May be {@code null}.
	 * @return the {@link Value}.
	 */
	public static <T extends V, V> LongScoredValue<V> fromNullable(long score, T value) {

		if (value == null) {
			return empty();
		}

		return new LongScoredValue<>(score, value);
	}

	/**
	 * Returns an empty {@code ScoredValue} instance. No value is present for this
	 * instance.
	 *
	 * @return the {@link LongScoredValue}
	 */
	@SuppressWarnings("unchecked")
	public static <V> LongScoredValue<V> empty() {
		return (LongScoredValue<V>) EMPTY;
	}

	/**
	 * Creates a {@link LongScoredValue} from a {@code key} and {@code value}. The
	 * resulting value contains the value.
	 *
	 * @param score the score.
	 * @param value the value. Must not be {@code null}.
	 * @return the {@link LongScoredValue}.
	 */
	public static <T extends V, V> LongScoredValue<V> just(long score, T value) {

		LettuceAssert.notNull(value, "Value must not be null");

		return new LongScoredValue<>(score, value);
	}

	public long getScore() {
		return score;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof LongScoredValue))
			return false;
		if (!super.equals(o))
			return false;

		LongScoredValue<?> that = (LongScoredValue<?>) o;

		return Long.compare(that.score, score) == 0;
	}

	@Override
	public int hashCode() {
		int result = (int) (score ^ (score >>> 32));
		result = 31 * result + (hasValue() ? getValue().hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return hasValue() ? String.format("LongScoredValue[%f, %s]", score, getValue())
				: String.format("LongScoredValue[%f].empty", score);
	}

	/**
	 * Returns a {@link LongScoredValue} consisting of the results of applying the
	 * given function to the value of this element. Mapping is performed only if a
	 * {@link #hasValue() value is present}.
	 *
	 * @param <R>    element type of the new {@link LongScoredValue}.
	 * @param mapper a stateless function to apply to each element.
	 * @return the new {@link LongScoredValue}.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <R> LongScoredValue<R> map(Function<? super V, ? extends R> mapper) {

		LettuceAssert.notNull(mapper, "Mapper function must not be null");

		if (hasValue()) {
			return new LongScoredValue<>(score, mapper.apply(getValue()));
		}

		return (LongScoredValue<R>) this;
	}

	/**
	 * Returns a {@link LongScoredValue} consisting of the results of applying the
	 * given function to the score of this element. Mapping is performed only if a
	 * {@link #hasValue() value is present}.
	 *
	 * @param mapper a stateless function to apply to each element.
	 * @return the new {@link LongScoredValue} .
	 */
	public LongScoredValue<V> mapScore(Function<? super Number, ? extends Number> mapper) {

		LettuceAssert.notNull(mapper, "Mapper function must not be null");

		if (hasValue()) {
			return new LongScoredValue<>(mapper.apply(score).longValue(), getValue());
		}

		return this;
	}

}
