package com.redis.lettucemod.api;

import com.redis.lettucemod.protocol.JsonCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class JsonGetOptions<K, V> implements CompositeArgument {

    private V indent;
    private V newline;
    private V space;
    private boolean noEscape;

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        if (indent != null) {
            args.add(JsonCommandKeyword.INDENT);
            args.addValue((V) indent);
        }
        if (newline != null) {
            args.add(JsonCommandKeyword.NEWLINE);
            args.addValue((V) newline);
        }
        if (space != null) {
            args.add(JsonCommandKeyword.SPACE);
            args.addValue((V) space);
        }
        if (noEscape) {
            args.add(JsonCommandKeyword.NOESCAPE);
        }
    }

}
