package com.redislabs.mesclun.search;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class IndexInfo<K> {
	private K indexName;
	private Double numDocs;
	private List<Object> indexOptions;
	private List<Field<K>> fields;
	private String maxDocId;
	private Long numTerms;
	private Long numRecords;
	private Double invertedSizeMb;
	private Long totalInvertedIndexBlocks;
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
}