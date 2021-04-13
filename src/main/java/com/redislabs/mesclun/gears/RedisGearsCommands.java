package com.redislabs.mesclun.gears;

import java.util.List;

public interface RedisGearsCommands<K, V> {

    String pyExecute(String function, PyExecuteOptions options);

    List<Registration> dumpRegistrations();

}
