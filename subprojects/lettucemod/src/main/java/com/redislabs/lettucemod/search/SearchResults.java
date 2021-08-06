package com.redislabs.lettucemod.search;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;

@Data
@EqualsAndHashCode(callSuper = true)
public class SearchResults<K, V> extends ArrayList<Document<K, V>> {

    private static final long serialVersionUID = 286617386389045710L;

    private long count;

}
