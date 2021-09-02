package com.redis.lettucemod.output;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OutputFactory {

    public static <T> List<T> newList(int capacity) {

        if (capacity < 1) {
            return Collections.emptyList();
        }

        return new ArrayList<>(capacity);
    }
}
