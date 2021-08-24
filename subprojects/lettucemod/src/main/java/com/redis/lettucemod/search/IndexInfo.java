package com.redis.lettucemod.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexInfo {
    private String indexName;
    private Double numDocs;
    private List<Object> indexOptions;
    private List<Field> fields;
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
