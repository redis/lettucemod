package com.redislabs.lettucemod.search.aggregate.reducers;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RandomSample extends AbstractPropertyReducer {

    private final int size;

    public RandomSample(String as, String property, int size) {
        super(as, property);
        this.size = size;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.RANDOM_SAMPLE);
        args.add(2);
        args.addProperty(property);
        args.add(size);
    }

    public static RandomSampleBuilder property(String property) {
        return new RandomSampleBuilder(property);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RandomSampleBuilder extends PropertyReducerBuilder<RandomSampleBuilder> {

        private int size;

        public RandomSampleBuilder(String property) {
            super(property);
        }

        public RandomSample build() {
            return new RandomSample(as, property, size);
        }
    }

}
