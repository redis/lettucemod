package com.redis.lettucemod.api.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.util.List;

@Data
@Builder
public class CreateOptions<K, V> implements RediSearchArgument<K, V> {

    public enum DataType {
        HASH, JSON
    }

    private DataType on;
    @Singular
    private List<K> prefixes;
    private V filter;
    private Language defaultLanguage;
    private K languageField;
    private Double defaultScore;
    private K scoreField;
    private K payloadField;
    private boolean maxTextFields;
    private Long temporary;
    private boolean noOffsets;
    private boolean noHL;
    private boolean noFields;
    private boolean noFreqs;
    private boolean noItitialScan;
    /**
     * set to empty list for STOPWORDS 0
     */
    private List<V> stopWords;

    @Override
    public void build(SearchCommandArgs<K, V> args) {
        if (on != null) {
            args.add(SearchCommandKeyword.ON);
            args.add(on.name());
        }
        if (prefixes != null && !prefixes.isEmpty()) {
            args.add(SearchCommandKeyword.PREFIX);
            args.add(prefixes.size());
            prefixes.forEach(args::addKey);
        }
        if (filter != null) {
            args.add(SearchCommandKeyword.FILTER);
            args.addValue(filter);
        }
        if (defaultLanguage != null) {
            args.add(SearchCommandKeyword.LANGUAGE);
            args.add(defaultLanguage.name());
        }
        if (languageField != null) {
            args.add(SearchCommandKeyword.LANGUAGE_FIELD);
            args.addKey(languageField);
        }
        if (defaultScore != null) {
            args.add(SearchCommandKeyword.SCORE);
            args.add(defaultScore);
        }
        if (scoreField != null) {
            args.add(SearchCommandKeyword.SCORE_FIELD);
            args.addKey(scoreField);
        }
        if (payloadField != null) {
            args.add(SearchCommandKeyword.PAYLOAD_FIELD);
            args.addKey(payloadField);
        }
        if (maxTextFields) {
            args.add(SearchCommandKeyword.MAXTEXTFIELDS);
        }
        if (temporary != null) {
            args.add(SearchCommandKeyword.TEMPORARY);
            args.add(temporary);
        }
        if (noOffsets) {
            args.add(SearchCommandKeyword.NOOFFSETS);
        }
        if (noHL) {
            args.add(SearchCommandKeyword.NOHL);
        }
        if (noFields) {
            args.add(SearchCommandKeyword.NOFIELDS);
        }
        if (noFreqs) {
            args.add(SearchCommandKeyword.NOFREQS);
        }
        if (noItitialScan) {
            args.add(SearchCommandKeyword.NOINITIALSCAN);
        }
        if (stopWords != null) {
            args.add(SearchCommandKeyword.STOPWORDS);
            args.add(stopWords.size());
            stopWords.forEach(args::addValue);
        }
    }

}
