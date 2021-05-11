package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import lombok.Builder;
import lombok.Singular;

import java.util.List;

import static com.redislabs.mesclun.search.protocol.CommandKeyword.LOAD;
import static com.redislabs.mesclun.search.protocol.CommandKeyword.VERBATIM;

@SuppressWarnings({"rawtypes", "unchecked"})
@Builder
public class AggregateOptions implements RediSearchArgument {

    private final boolean verbatim;
    @Singular
    private final List<String> loads;
    @Singular
    private final List<Operation> operations;

    @SuppressWarnings("rawtypes")
    @Override
    public void build(RediSearchCommandArgs args) {
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
        for (Operation operation : operations) {
            operation.build(args);
        }
    }

    public interface Reducer extends RediSearchArgument {
    }

    public interface Operation extends RediSearchArgument {

    }
}
