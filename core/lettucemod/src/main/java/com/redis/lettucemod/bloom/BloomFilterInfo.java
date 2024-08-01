package com.redis.lettucemod.bloom;

public class BloomFilterInfo {

	private long capacity;
	private long size;
	private long numFilters;
	private long numInserted;
	private long expansionRate;

	public long getCapacity() {
		return capacity;
	}

	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getNumFilters() {
		return numFilters;
	}

	public void setNumFilters(long numFilters) {
		this.numFilters = numFilters;
	}

	public long getNumInserted() {
		return numInserted;
	}

	public void setNumInserted(long numInserted) {
		this.numInserted = numInserted;
	}

	public long getExpansionRate() {
		return expansionRate;
	}

	public void setExpansionRate(long expansionRate) {
		this.expansionRate = expansionRate;
	}
}
