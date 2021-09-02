package com.redis.lettucemod.api.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import lombok.Builder;
import lombok.Data;

@SuppressWarnings("rawtypes")
@Data
@Builder
public class SuggetOptions implements RediSearchArgument {

    private boolean fuzzy;
    private boolean withScores;
    private boolean withPayloads;
    private Long max;

    @Override
    public void build(SearchCommandArgs args) {
        if (fuzzy) {
            args.add(SearchCommandKeyword.FUZZY);
        }
        if (withScores) {
            args.add(SearchCommandKeyword.WITHSCORES);
        }
        if (withPayloads) {
            args.add(SearchCommandKeyword.WITHPAYLOADS);
        }
        if (max != null) {
            args.add(SearchCommandKeyword.MAX);
            args.add(max);
        }
    }

}
