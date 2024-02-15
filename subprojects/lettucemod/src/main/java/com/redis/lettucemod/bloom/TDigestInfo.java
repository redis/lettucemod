package com.redis.lettucemod.bloom;

public class TDigestInfo {

	private long compression;
	private long capacity;
	private long mergedNodes;
	private long unmergedNodes;
	private long mergedWeight;
	private long unmergedWeight;
	private long observations;
	private long totalCompressions;
	private long memoryUsage;

	public long getCompression() {
		return compression;
	}

	public void setCompression(long compression) {
		this.compression = compression;
	}

	public long getCapacity() {
		return capacity;
	}

	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}

	public long getMergedNodes() {
		return mergedNodes;
	}

	public void setMergedNodes(long mergedNodes) {
		this.mergedNodes = mergedNodes;
	}

	public long getUnmergedNodes() {
		return unmergedNodes;
	}

	public void setUnmergedNodes(long unmergedNodes) {
		this.unmergedNodes = unmergedNodes;
	}

	public long getMergedWeight() {
		return mergedWeight;
	}

	public void setMergedWeight(long mergedWeight) {
		this.mergedWeight = mergedWeight;
	}

	public long getUnmergedWeight() {
		return unmergedWeight;
	}

	public void setUnmergedWeight(long unmergedWeight) {
		this.unmergedWeight = unmergedWeight;
	}

	public long getObservations() {
		return observations;
	}

	public void setObservations(long observations) {
		this.observations = observations;
	}

	public long getTotalCompressions() {
		return totalCompressions;
	}

	public void setTotalCompressions(long totalCompressions) {
		this.totalCompressions = totalCompressions;
	}

	public long getMemoryUsage() {
		return memoryUsage;
	}

	public void setMemoryUsage(long memoryUsage) {
		this.memoryUsage = memoryUsage;
	}
}
