package com.redis.lettucemod.gears;

import java.util.List;

public class ExecutionDetails {

	private String shardId;
	private ExecutionPlan plan;

	public String getShardId() {
		return shardId;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	public ExecutionPlan getPlan() {
		return plan;
	}

	public void setPlan(ExecutionPlan plan) {
		this.plan = plan;
	}

	public static class ExecutionPlan {

		private String status;
		private long shardsReceived;
		private long shardsCompleted;
		private long results;
		private long errors;
		private long totalDuration;
		private long readDuration;
		private List<Step> steps;

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public long getShardsReceived() {
			return shardsReceived;
		}

		public void setShardsReceived(long shardsReceived) {
			this.shardsReceived = shardsReceived;
		}

		public long getShardsCompleted() {
			return shardsCompleted;
		}

		public void setShardsCompleted(long shardsCompleted) {
			this.shardsCompleted = shardsCompleted;
		}

		public long getResults() {
			return results;
		}

		public void setResults(long results) {
			this.results = results;
		}

		public long getErrors() {
			return errors;
		}

		public void setErrors(long errors) {
			this.errors = errors;
		}

		public long getTotalDuration() {
			return totalDuration;
		}

		public void setTotalDuration(long totalDuration) {
			this.totalDuration = totalDuration;
		}

		public long getReadDuration() {
			return readDuration;
		}

		public void setReadDuration(long readDuration) {
			this.readDuration = readDuration;
		}

		public List<Step> getSteps() {
			return steps;
		}

		public void setSteps(List<Step> steps) {
			this.steps = steps;
		}

		public static class Step {

			private String type;
			private long duration;
			private String name;
			private String arg;

			public String getType() {
				return type;
			}

			public void setType(String type) {
				this.type = type;
			}

			public long getDuration() {
				return duration;
			}

			public void setDuration(long duration) {
				this.duration = duration;
			}

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}

			public String getArg() {
				return arg;
			}

			public void setArg(String arg) {
				this.arg = arg;
			}

		}

	}

}
