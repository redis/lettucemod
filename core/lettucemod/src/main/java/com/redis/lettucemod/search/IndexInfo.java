package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.List;

import lombok.ToString;

@ToString
public class IndexInfo {

	private String indexName;

	private Double numDocs;

	private CreateOptions<String, String> indexOptions;

	private List<Field<String>> fields;

	private String maxDocId;

	private Long numTerms;

	private Long numRecords;

	private Double invertedSizeMb;

	private Long totalInvertedIndexBlocks;

	private Double vectorIndexSizeMb;

	private Double offsetVectorsSizeMb;

	private Double docTableSizeMb;

	private Double sortableValuesSizeMb;

	private Double keyTableSizeMb;

	private Double recordsPerDocAvg;

	private Double bytesPerRecordAvg;

	private Double offsetsPerTermAvg;

	private Double offsetBitsPerRecordAvg;

	private List<Object> gcStats;

	private List<Object> cursorStats;

	public IndexInfo() {
	}

	private IndexInfo(Builder builder) {
		this.indexName = builder.indexName;
		this.numDocs = builder.numDocs;
		this.indexOptions = builder.indexOptions;
		this.fields = builder.fields;
		this.maxDocId = builder.maxDocId;
		this.numTerms = builder.numTerms;
		this.numRecords = builder.numRecords;
		this.invertedSizeMb = builder.invertedSizeMb;
		this.totalInvertedIndexBlocks = builder.totalInvertedIndexBlocks;
		this.vectorIndexSizeMb = builder.vectorIndexSizeMb;
		this.offsetVectorsSizeMb = builder.offsetVectorsSizeMb;
		this.docTableSizeMb = builder.docTableSizeMb;
		this.sortableValuesSizeMb = builder.sortableValuesSizeMb;
		this.keyTableSizeMb = builder.keyTableSizeMb;
		this.recordsPerDocAvg = builder.recordsPerDocAvg;
		this.bytesPerRecordAvg = builder.bytesPerRecordAvg;
		this.offsetsPerTermAvg = builder.offsetsPerTermAvg;
		this.offsetBitsPerRecordAvg = builder.offsetBitsPerRecordAvg;
		this.gcStats = builder.gcStats;
		this.cursorStats = builder.cursorStats;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public Double getNumDocs() {
		return numDocs;
	}

	public void setNumDocs(Double numDocs) {
		this.numDocs = numDocs;
	}

	public CreateOptions<String, String> getIndexOptions() {
		return indexOptions;
	}

	public void setIndexOptions(CreateOptions<String, String> indexOptions) {
		this.indexOptions = indexOptions;
	}

	public List<Field<String>> getFields() {
		return fields;
	}

	public void setFields(List<Field<String>> fields) {
		this.fields = fields;
	}

	public String getMaxDocId() {
		return maxDocId;
	}

	public void setMaxDocId(String maxDocId) {
		this.maxDocId = maxDocId;
	}

	public Long getNumTerms() {
		return numTerms;
	}

	public void setNumTerms(Long numTerms) {
		this.numTerms = numTerms;
	}

	public Long getNumRecords() {
		return numRecords;
	}

	public void setNumRecords(Long numRecords) {
		this.numRecords = numRecords;
	}

	public Double getInvertedSizeMb() {
		return invertedSizeMb;
	}

	public void setInvertedSizeMb(Double invertedSizeMb) {
		this.invertedSizeMb = invertedSizeMb;
	}

	public Long getTotalInvertedIndexBlocks() {
		return totalInvertedIndexBlocks;
	}

	public void setTotalInvertedIndexBlocks(Long totalInvertedIndexBlocks) {
		this.totalInvertedIndexBlocks = totalInvertedIndexBlocks;
	}

	public Double getVectorIndexSizeMb() {
		return vectorIndexSizeMb;
	}

	public void setVectorIndexSizeMb(Double vectorIndexSizeMb) {
		this.vectorIndexSizeMb = vectorIndexSizeMb;
	}

	public Double getOffsetVectorsSizeMb() {
		return offsetVectorsSizeMb;
	}

	public void setOffsetVectorsSizeMb(Double offsetVectorsSizeMb) {
		this.offsetVectorsSizeMb = offsetVectorsSizeMb;
	}

	public Double getDocTableSizeMb() {
		return docTableSizeMb;
	}

	public void setDocTableSizeMb(Double docTableSizeMb) {
		this.docTableSizeMb = docTableSizeMb;
	}

	public Double getSortableValuesSizeMb() {
		return sortableValuesSizeMb;
	}

	public void setSortableValuesSizeMb(Double sortableValuesSizeMb) {
		this.sortableValuesSizeMb = sortableValuesSizeMb;
	}

	public Double getKeyTableSizeMb() {
		return keyTableSizeMb;
	}

	public void setKeyTableSizeMb(Double keyTableSizeMb) {
		this.keyTableSizeMb = keyTableSizeMb;
	}

	public Double getRecordsPerDocAvg() {
		return recordsPerDocAvg;
	}

	public void setRecordsPerDocAvg(Double recordsPerDocAvg) {
		this.recordsPerDocAvg = recordsPerDocAvg;
	}

	public Double getBytesPerRecordAvg() {
		return bytesPerRecordAvg;
	}

	public void setBytesPerRecordAvg(Double bytesPerRecordAvg) {
		this.bytesPerRecordAvg = bytesPerRecordAvg;
	}

	public Double getOffsetsPerTermAvg() {
		return offsetsPerTermAvg;
	}

	public void setOffsetsPerTermAvg(Double offsetsPerTermAvg) {
		this.offsetsPerTermAvg = offsetsPerTermAvg;
	}

	public Double getOffsetBitsPerRecordAvg() {
		return offsetBitsPerRecordAvg;
	}

	public void setOffsetBitsPerRecordAvg(Double offsetBitsPerRecordAvg) {
		this.offsetBitsPerRecordAvg = offsetBitsPerRecordAvg;
	}

	public List<Object> getGcStats() {
		return gcStats;
	}

	public void setGcStats(List<Object> gcStats) {
		this.gcStats = gcStats;
	}

	public List<Object> getCursorStats() {
		return cursorStats;
	}

	public void setCursorStats(List<Object> cursorStats) {
		this.cursorStats = cursorStats;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private String indexName;

		private Double numDocs;

		private CreateOptions<String, String> indexOptions;

		private List<Field<String>> fields = new ArrayList<>();

		private String maxDocId;

		private Long numTerms;

		private Long numRecords;

		private Double invertedSizeMb;

		private Long totalInvertedIndexBlocks;

		private Double vectorIndexSizeMb;

		private Double offsetVectorsSizeMb;

		private Double docTableSizeMb;

		private Double sortableValuesSizeMb;

		private Double keyTableSizeMb;

		private Double recordsPerDocAvg;

		private Double bytesPerRecordAvg;

		private Double offsetsPerTermAvg;

		private Double offsetBitsPerRecordAvg;

		private List<Object> gcStats = new ArrayList<>();

		private List<Object> cursorStats = new ArrayList<>();

		private Builder() {
		}

		public Builder indexName(String indexName) {
			this.indexName = indexName;
			return this;
		}

		public Builder numDocs(Double numDocs) {
			this.numDocs = numDocs;
			return this;
		}

		public Builder indexOptions(CreateOptions<String, String> indexOptions) {
			this.indexOptions = indexOptions;
			return this;
		}

		public Builder fields(List<Field<String>> fields) {
			this.fields = fields;
			return this;
		}

		public Builder maxDocId(String maxDocId) {
			this.maxDocId = maxDocId;
			return this;
		}

		public Builder numTerms(Long numTerms) {
			this.numTerms = numTerms;
			return this;
		}

		public Builder numRecords(Long numRecords) {
			this.numRecords = numRecords;
			return this;
		}

		public Builder invertedSizeMb(Double invertedSizeMb) {
			this.invertedSizeMb = invertedSizeMb;
			return this;
		}

		public Builder totalInvertedIndexBlocks(Long totalInvertedIndexBlocks) {
			this.totalInvertedIndexBlocks = totalInvertedIndexBlocks;
			return this;
		}

		public Builder vectorIndexSizeMb(Double vectorIndexSizeMb) {
			this.vectorIndexSizeMb = vectorIndexSizeMb;
			return this;
		}

		public Builder offsetVectorsSizeMb(Double offsetVectorsSizeMb) {
			this.offsetVectorsSizeMb = offsetVectorsSizeMb;
			return this;
		}

		public Builder docTableSizeMb(Double docTableSizeMb) {
			this.docTableSizeMb = docTableSizeMb;
			return this;
		}

		public Builder sortableValuesSizeMb(Double sortableValuesSizeMb) {
			this.sortableValuesSizeMb = sortableValuesSizeMb;
			return this;
		}

		public Builder keyTableSizeMb(Double keyTableSizeMb) {
			this.keyTableSizeMb = keyTableSizeMb;
			return this;
		}

		public Builder recordsPerDocAvg(Double recordsPerDocAvg) {
			this.recordsPerDocAvg = recordsPerDocAvg;
			return this;
		}

		public Builder bytesPerRecordAvg(Double bytesPerRecordAvg) {
			this.bytesPerRecordAvg = bytesPerRecordAvg;
			return this;
		}

		public Builder offsetsPerTermAvg(Double offsetsPerTermAvg) {
			this.offsetsPerTermAvg = offsetsPerTermAvg;
			return this;
		}

		public Builder offsetBitsPerRecordAvg(Double offsetBitsPerRecordAvg) {
			this.offsetBitsPerRecordAvg = offsetBitsPerRecordAvg;
			return this;
		}

		public Builder gcStats(List<Object> gcStats) {
			this.gcStats = gcStats;
			return this;
		}

		public Builder cursorStats(List<Object> cursorStats) {
			this.cursorStats = cursorStats;
			return this;
		}

		public IndexInfo build() {
			return new IndexInfo(this);
		}

	}

}
