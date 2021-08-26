package com.redis.lettucemod.search;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.*;

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
    public void build(RediSearchCommandArgs<K, V> args) {
        if (on != null) {
            args.add(CommandKeyword.ON);
            args.add(on.name());
        }
        if (prefixes != null) {
            args.add(CommandKeyword.PREFIX);
            args.add(prefixes.size());
            prefixes.forEach(args::addKey);
        }
        if (filter != null) {
            args.add(CommandKeyword.FILTER);
            args.addValue(filter);
        }
        if (defaultLanguage != null) {
            args.add(CommandKeyword.LANGUAGE);
            args.add(defaultLanguage.name());
        }
        if (languageField != null) {
            args.add(CommandKeyword.LANGUAGE_FIELD);
            args.addKey(languageField);
        }
        if (defaultScore != null) {
            args.add(CommandKeyword.SCORE);
            args.add(defaultScore);
        }
        if (scoreField != null) {
            args.add(CommandKeyword.SCORE_FIELD);
            args.addKey(scoreField);
        }
        if (payloadField != null) {
            args.add(CommandKeyword.PAYLOAD_FIELD);
            args.addKey(payloadField);
        }
        if (maxTextFields) {
            args.add(CommandKeyword.MAXTEXTFIELDS);
        }
        if (temporary != null) {
            args.add(CommandKeyword.TEMPORARY);
            args.add(temporary);
        }
        if (noOffsets) {
            args.add(CommandKeyword.NOOFFSETS);
        }
        if (noHL) {
            args.add(CommandKeyword.NOHL);
        }
        if (noFields) {
            args.add(CommandKeyword.NOFIELDS);
        }
        if (noFreqs) {
            args.add(CommandKeyword.NOFREQS);
        }
        if (noItitialScan) {
            args.add(CommandKeyword.NOINITIALSCAN);
        }
        if (stopWords != null) {
            args.add(CommandKeyword.STOPWORDS);
            args.add(stopWords.size());
            stopWords.forEach(args::addValue);
        }
    }

}
