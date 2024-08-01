package com.redis.lettucemod.bloom;

public class CuckooFilter {

	private Long size;
	private Long numBuckets;
	private Long numFilters;
	private Long numItemsInserted;
	private Long numItemsDeleted;
	private Long bucketSize;
	private Long expansionRate;
	private Long maxIteration;

	public Long getSize() {
		return size;
	}

	public Long getNumBuckets() {
		return numBuckets;
	}

	public Long getNumFilters() {
		return numFilters;
	}

	public Long getNumItemsInserted() {
		return numItemsInserted;
	}

	public Long getNumItemsDeleted() {
		return numItemsDeleted;
	}

	public Long getBucketSize() {
		return bucketSize;
	}

	public Long getExpansionRate() {
		return expansionRate;
	}

	public Long getMaxIteration() {
		return maxIteration;
	}

	public void setSize(Long size) {
		this.size = size;
	}

	public void setNumBuckets(Long numBuckets) {
		this.numBuckets = numBuckets;
	}

	public void setNumFilters(Long numFilters) {
		this.numFilters = numFilters;
	}

	public void setNumItemsInserted(Long numItemsInserted) {
		this.numItemsInserted = numItemsInserted;
	}

	public void setNumItemsDeleted(Long numItemsDeleted) {
		this.numItemsDeleted = numItemsDeleted;
	}

	public void setBucketSize(Long bucketSize) {
		this.bucketSize = bucketSize;
	}

	public void setExpansionRate(Long expansionRate) {
		this.expansionRate = expansionRate;
	}

	public void setMaxIteration(Long maxIteration) {
		this.maxIteration = maxIteration;
	}
}
