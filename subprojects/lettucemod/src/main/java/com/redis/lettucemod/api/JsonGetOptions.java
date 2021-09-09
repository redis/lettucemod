package com.redis.lettucemod.api;

import com.redis.lettucemod.protocol.JsonCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class JsonGetOptions implements CompositeArgument {

    private String indent;
    private String newline;
    private String space;
    private boolean noEscape;

    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        if (indent != null) {
            args.add(JsonCommandKeyword.INDENT);
            args.add(indent);
        }
        if (newline != null) {
            args.add(JsonCommandKeyword.NEWLINE);
            args.add(newline);
        }
        if (space != null) {
            args.add(JsonCommandKeyword.SPACE);
            args.add(space);
        }
        if (noEscape) {
            args.add(JsonCommandKeyword.NOESCAPE);
        }
    }

}
