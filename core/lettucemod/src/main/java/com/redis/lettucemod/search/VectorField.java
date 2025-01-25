package com.redis.lettucemod.search;

import java.util.Objects;
import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

import lombok.ToString;

@ToString
public class VectorField<K> extends Field<K> {

	private final SearchCommandKeyword algorithm;
	private final SearchCommandKeyword vectorType;
	private final int dim;
	private final SearchCommandKeyword distanceMetric;

	private final Optional<Integer> initialCap;
	private final Optional<Integer> blockSize; // 1024

	private final Optional<Integer> m; // 16

	private final Optional<Integer> efConstruction; // 200

	private final Optional<Integer> efRuntime; // 10

	private final Optional<Float> epsilon; // 0.01

	private VectorField(Builder<K> builder) {
		super(Type.VECTOR, builder);
		this.algorithm = builder.algorithm;
		this.vectorType = builder.vectorType;
		this.dim = builder.dim;
		this.distanceMetric = builder.distanceMetric;
		this.initialCap = Optional.ofNullable(builder.initialCap);
		this.blockSize = Optional.ofNullable(builder.blockSize);
		this.m = Optional.ofNullable(builder.m);
		this.efConstruction = Optional.ofNullable(builder.efConstruction);
		this.efRuntime = Optional.ofNullable(builder.efRuntime);
		this.epsilon = Optional.ofNullable(builder.epsilon);
	}

	public static <K> Builder<K> name(K name) {
		return new Builder<>(name);
	}

	@Override
	protected void buildField(SearchCommandArgs<K, Object> args) {
		args.add(SearchCommandKeyword.VECTOR).add(algorithm).add(getOptionSize()).add(SearchCommandKeyword.TYPE)
				.add(vectorType).add(SearchCommandKeyword.DIM).addValue(String.valueOf(dim))
				.add(SearchCommandKeyword.DISTANCE_METRIC).add(distanceMetric);

		initialCap.ifPresent(i -> args.add(SearchCommandKeyword.INITIAL_CAP).add(i));

		if (SearchCommandKeyword.FLAT.equals(algorithm)) {
			blockSize.ifPresent(i -> args.add(SearchCommandKeyword.BLOCK_SIZE).add(i));
		} else if (SearchCommandKeyword.HNSW.equals(algorithm)) {
			m.ifPresent(i -> args.add(SearchCommandKeyword.M).add(i));
			efConstruction.ifPresent(i -> args.add(SearchCommandKeyword.EF_CONSTRUCTION).add(i));
			efRuntime.ifPresent(i -> args.add(SearchCommandKeyword.EF_RUNTIME).add(i));
			epsilon.ifPresent(f -> args.add(SearchCommandKeyword.EPSILON).add(f));
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		if (!super.equals(o))
			return false;
		VectorField<?> that = (VectorField<?>) o;
		return dim == that.dim && algorithm == that.algorithm && vectorType == that.vectorType
				&& distanceMetric == that.distanceMetric && Objects.equals(initialCap, that.initialCap)
				&& Objects.equals(blockSize, that.blockSize) && Objects.equals(m, that.m)
				&& Objects.equals(efConstruction, that.efConstruction) && Objects.equals(efRuntime, that.efRuntime)
				&& Objects.equals(epsilon, that.epsilon);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), algorithm, vectorType, dim, distanceMetric, initialCap, blockSize, m,
				efConstruction, efRuntime, epsilon);
	}

	private int getOptionSize() {
		int optionSize = 6;
		if (SearchCommandKeyword.FLAT.equals(algorithm)) {
			if (initialCap.isPresent()) {
				optionSize += 2;
			}
			if (blockSize.isPresent()) {
				optionSize += 2;
			}
			return optionSize;
		} else if (SearchCommandKeyword.HNSW.equals(algorithm)) {
			if (initialCap.isPresent()) {
				optionSize += 2;
			}
			if (m.isPresent()) {
				optionSize += 2;
			}
			if (efConstruction.isPresent()) {
				optionSize += 2;
			}
			if (efRuntime.isPresent()) {
				optionSize += 2;
			}
			if (epsilon.isPresent()) {
				optionSize += 2;
			}
			return optionSize;
		}
		throw new IllegalArgumentException("Unknown vector algorithm type: " + algorithm);
	}

	public static class Builder<K> extends Field.Builder<K, Builder<K>> {
		private SearchCommandKeyword algorithm;
		private SearchCommandKeyword vectorType;
		private int dim;
		private SearchCommandKeyword distanceMetric;

		private Integer initialCap;
		private Integer blockSize;

		private Integer m;

		private Integer efConstruction;

		private Integer efRuntime;

		private Float epsilon;

		public Builder(K name) {
			super(name);
		}

		public Builder<K> algorithm(SearchCommandKeyword algorithm) {
			this.algorithm = algorithm;
			return this;
		}

		public Builder<K> vectorType(SearchCommandKeyword type) {
			this.vectorType = type;
			return this;
		}

		public Builder<K> dim(int dim) {
			this.dim = dim;
			return this;
		}

		public Builder<K> distanceMetric(SearchCommandKeyword distanceMetric) {
			this.distanceMetric = distanceMetric;
			return this;
		}

		public Builder<K> initialCap(Integer initialCap) {
			this.initialCap = initialCap;
			return this;
		}

		public Builder<K> blockSize(Integer blockSize) {
			this.blockSize = blockSize;
			return this;
		}

		public Builder<K> m(Integer m) {
			this.m = m;
			return this;
		}

		public Builder<K> efConstruction(Integer efConstruction) {
			this.efConstruction = efConstruction;
			return this;
		}

		public Builder<K> efRuntime(Integer efRuntime) {
			this.efRuntime = efRuntime;
			return this;
		}

		public Builder<K> epsilon(Float epsilon) {
			this.epsilon = epsilon;
			return this;
		}

		public VectorField<K> build() {
			return new VectorField<>(this);
		}
	}
}