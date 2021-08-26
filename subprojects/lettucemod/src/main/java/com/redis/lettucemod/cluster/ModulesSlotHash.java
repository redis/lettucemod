package com.redis.lettucemod.cluster;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.RedisCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModulesSlotHash {

    /**
     * TODO submit a Pull Request against lettuce-code to make the {@link SlotHash} partition method public
     */
    public static <K, V> Map<Integer, List<K>> partition(RedisCodec<K, V> codec, Iterable<K> keys) {
        Map<Integer, List<K>> partitioned = new HashMap<>();
        for (K key : keys) {
            int slot = SlotHash.getSlot(codec.encodeKey(key));
            if (!partitioned.containsKey(slot)) {
                partitioned.put(slot, new ArrayList<>());
            }
            Collection<K> list = partitioned.get(slot);
            list.add(key);
        }
        return partitioned;
    }

    /**
     * TODO submit a Pull Request against lettuce-code to make the {@link SlotHash} getSlots method public
     */
    public static <K> Map<K, Integer> getSlots(Map<Integer, ? extends Iterable<K>> partitioned) {

        Map<K, Integer> result = new HashMap<>();
        for (Map.Entry<Integer, ? extends Iterable<K>> entry : partitioned.entrySet()) {
            for (K key : entry.getValue()) {
                result.put(key, entry.getKey());
            }
        }

        return result;
    }
}
