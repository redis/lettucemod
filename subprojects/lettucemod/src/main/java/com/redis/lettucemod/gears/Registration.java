package com.redis.lettucemod.gears;

import java.util.Map;

public class Registration {

	private String id;
	private String reader;
	private String description;
	private Data data;
	private String privateData;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getReader() {
		return reader;
	}

	public void setReader(String reader) {
		this.reader = reader;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

	public String getPrivateData() {
		return privateData;
	}

	public void setPrivateData(String privateData) {
		this.privateData = privateData;
	}

	public static class Data {
		private String mode;
		private long numTriggered;
		private long numSuccess;
		private long numFailures;
		private long numAborted;
		private long lastRunDurationMS;
		private long totalRunDurationMS;
		private long avgRunDurationMS;
		private long lastEstimatedLagMS;
		private long avgEstimatedLagMS;
		private String lastError;
		private Map<String, Object> args;
		private String status;

		public String getMode() {
			return mode;
		}

		public void setMode(String mode) {
			this.mode = mode;
		}

		public long getNumTriggered() {
			return numTriggered;
		}

		public void setNumTriggered(long numTriggered) {
			this.numTriggered = numTriggered;
		}

		public long getNumSuccess() {
			return numSuccess;
		}

		public void setNumSuccess(long numSuccess) {
			this.numSuccess = numSuccess;
		}

		public long getNumFailures() {
			return numFailures;
		}

		public void setNumFailures(long numFailures) {
			this.numFailures = numFailures;
		}

		public long getNumAborted() {
			return numAborted;
		}

		public void setNumAborted(long numAborted) {
			this.numAborted = numAborted;
		}

		public String getLastError() {
			return lastError;
		}

		public void setLastError(String lastError) {
			this.lastError = lastError;
		}

		public Map<String, Object> getArgs() {
			return args;
		}

		public void setArgs(Map<String, Object> args) {
			this.args = args;
		}

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public long getLastRunDurationMS() {
			return lastRunDurationMS;
		}

		public void setLastRunDurationMS(long lastRunDurationMS) {
			this.lastRunDurationMS = lastRunDurationMS;
		}

		public long getTotalRunDurationMS() {
			return totalRunDurationMS;
		}

		public void setTotalRunDurationMS(long totalRunDurationMS) {
			this.totalRunDurationMS = totalRunDurationMS;
		}

		public long getAvgRunDurationMS() {
			return avgRunDurationMS;
		}

		public void setAvgRunDurationMS(long avgRunDurationMS) {
			this.avgRunDurationMS = avgRunDurationMS;
		}

		public long getLastEstimatedLagMS() {
			return lastEstimatedLagMS;
		}

		public void setLastEstimatedLagMS(long lastEstimatedLagMS) {
			this.lastEstimatedLagMS = lastEstimatedLagMS;
		}

		public long getAvgEstimatedLagMS() {
			return avgEstimatedLagMS;
		}

		public void setAvgEstimatedLagMS(long avgEstimatedLagMS) {
			this.avgEstimatedLagMS = avgEstimatedLagMS;
		}

	}
}
