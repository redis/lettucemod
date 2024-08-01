package com.redis.lettucemod.bloom;

public class TopKInfo {

	private long k;
	private long width;
	private long depth;
	private double decay;

	public long getK() {
		return k;
	}

	public void setK(long k) {
		this.k = k;
	}

	public long getWidth() {
		return width;
	}

	public void setWidth(long width) {
		this.width = width;
	}

	public long getDepth() {
		return depth;
	}

	public void setDepth(long depth) {
		this.depth = depth;
	}

	public double getDecay() {
		return decay;
	}

	public void setDecay(double decay) {
		this.decay = decay;
	}
}
