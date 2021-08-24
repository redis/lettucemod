package com.redis.lettucemod.search;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;

@Data
@EqualsAndHashCode(callSuper = true)
public class SearchResults<K, V> extends ArrayList<Document<K, V>> {

    private long count;

}
