package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.util.List;

import static com.redislabs.mesclun.search.protocol.CommandKeyword.*;

@Data
@Builder
public class CreateOptions<K, V> implements RediSearchArgument {

    public enum Structure {
        HASH
    }

    private Structure on;
    @Singular
    private List<K> prefixes;
    private String filter;
    private SearchOptions.Language defaultLanguage;
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void build(RediSearchCommandArgs args) {
        if (on != null) {
            args.add(ON);
            args.add(on.name());
        }
        if (prefixes != null) {
            args.add(PREFIX);
            args.add(prefixes.size());
            prefixes.forEach(args::addKey);
        }
        if (filter != null) {
            args.add(FILTER);
            args.add(filter);
        }
        if (defaultLanguage != null) {
            args.add(LANGUAGE);
            args.add(defaultLanguage.name());
        }
        if (languageField != null) {
            args.add(LANGUAGE_FIELD);
            args.addKey(languageField);
        }
        if (defaultScore != null) {
            args.add(SCORE);
            args.add(defaultScore);
        }
        if (scoreField != null) {
            args.add(SCORE_FIELD);
            args.addKey(scoreField);
        }
        if (payloadField != null) {
            args.add(PAYLOAD_FIELD);
            args.addKey(payloadField);
        }
        if (maxTextFields) {
            args.add(MAXTEXTFIELDS);
        }
        if (temporary != null) {
            args.add(TEMPORARY);
            args.add(temporary);
        }
        if (noOffsets) {
            args.add(NOOFFSETS);
        }
        if (noHL) {
            args.add(NOHL);
        }
        if (noFields) {
            args.add(NOFIELDS);
        }
        if (noFreqs) {
            args.add(NOFREQS);
        }
        if (noItitialScan) {
            args.add(NOINITIALSCAN);
        }
        if (stopWords != null) {
            args.add(STOPWORDS);
            args.add(stopWords.size());
            stopWords.forEach(args::addValue);
        }
    }

}
