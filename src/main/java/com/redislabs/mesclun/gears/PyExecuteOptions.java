package com.redislabs.mesclun.gears;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Builder;
import lombok.Data;

import static com.redislabs.mesclun.gears.protocol.CommandKeyword.REQUIREMENTS;
import static com.redislabs.mesclun.gears.protocol.CommandKeyword.UNBLOCKING;

@Data
@Builder
public class PyExecuteOptions implements CompositeArgument {

    private boolean unblocking;
    private String requirements;

    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        if (unblocking) {
            args.add(UNBLOCKING);
        }
        if (requirements != null) {
            args.add(REQUIREMENTS);
            args.add(requirements);
        }
    }

}
