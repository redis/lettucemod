package com.redislabs.lettucemod.search;

import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.Builder;
import lombok.Singular;

import java.util.List;

import static com.redislabs.lettucemod.search.protocol.CommandKeyword.LOAD;
import static com.redislabs.lettucemod.search.protocol.CommandKeyword.VERBATIM;

@Builder
public class AggregateOptions<K, V> implements RediSearchArgument<K, V> {

    private final boolean verbatim;
    @Singular
    private final List<String> loads;
    @Singular
    private final List<Operation<K, V>> operations;

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        if (verbatim) {
            args.add(VERBATIM);
        }
        if (!loads.isEmpty()) {
            args.add(LOAD);
            args.add(loads.size());
            for (String load : loads) {
                args.addProperty(load);
            }
        }
        for (Operation<K, V> operation : operations) {
            operation.build(args);
        }
    }

    @SuppressWarnings("rawtypes")
    public interface Reducer extends RediSearchArgument {
    }

    public interface Operation<K, V> extends RediSearchArgument<K, V> {
    }
}
