package com.redis.lettucemod.bloom;

import com.redis.lettucemod.RedisModulesCommandBuilder;
import com.redis.lettucemod.cms.CmsInfo;
import com.redis.lettucemod.output.*;
import com.redis.lettucemod.protocol.*;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Value;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BloomCommandBuilder <K,V> extends RedisModulesCommandBuilder<K,V> {
    public BloomCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected <A, B, T> Command<A, B, T> createCommand(BloomFilterCommandType type, CommandOutput<A, B, T> output,
                                                       CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    protected <A, B, T> Command<A, B, T> createCommand(CuckooFilterCommandType type, CommandOutput<A, B, T> output,
                                                       CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    protected <A, B, T> Command<A, B, T> createCommand(CountMinSketchCommandType type, CommandOutput<A, B, T> output,
                                                       CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    protected <A, B, T> Command<A, B, T> createCommand(TopKCommandType type, CommandOutput<A, B, T> output,
                                                       CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    protected <A, B, T> Command<A, B, T> createCommand(TDigestCommandType type, CommandOutput<A, B, T> output,
                                                       CommandArgs<A, B> args) {
        return new Command<>(type, output, args);
    }

    public Command<K, V, Boolean> bfAdd(K key, V item){
        CommandArgs<K, V> args = args(key);
        args.addValue(item);
        return createCommand(BloomFilterCommandType.ADD, new BooleanOutput<>(codec), args);
    }
    public Command<K, V,Long> bfCard(K key){
        CommandArgs<K, V> args = args(key);
        return createCommand(BloomFilterCommandType.CARD, new IntegerOutput<>(codec),args);
    }
    public Command<K,V,Boolean> bfExists(K key, V item){
        CommandArgs<K,V> args = args(key);
        args.addValue(item);
        return createCommand(BloomFilterCommandType.EXISTS, new BooleanOutput<>(codec), args);
    }
    public Command<K,V,BfInfo> bfInfo(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(BloomFilterCommandType.INFO, new BfInfoOutput<>(codec), args);
    }
    public Command<K,V,Long> bfInfo(K key, BfInfoType infoType){
        CommandArgs<K,V> args = args(key);
        args.add(infoType.name());
        return createCommand(BloomFilterCommandType.INFO, new IntegerOutput<>(codec), args);
    }
    public Command<K,V, List<Boolean>> bfInsert(K key, V[] items){
        CommandArgs<K,V> args = args(key);
        for(V item : items){
            args.addValue(item);
        }

        return createCommand(BloomFilterCommandType.INSERT, new BooleanListOutput<>(codec), args);
    }

    public Command<K,V,List<Boolean>> bfInsert(K key, V[] items, BfInsertOptions options){
        CommandArgs<K,V> args = args(key);
        options.build(args);
        args.add("ITEMS");
        for(V item : items){
            args.addValue(item);
        }

        return createCommand(BloomFilterCommandType.INSERT, new BooleanListOutput<>(codec), args);
    }

    public Command<K,V,List<Boolean>> bfMAdd(K key, V[] items){
        CommandArgs<K,V> args = args(key);
        for(V item : items){
            args.addValue(item);
        }

        return createCommand(BloomFilterCommandType.MADD, new BooleanListOutput<>(codec), args);
    }
    public Command<K,V,List<Boolean>> bfMExists(K key, V[] items){
        CommandArgs<K,V> args = args(key);
        for(V item : items){
            args.addValue(item);
        }

        return createCommand(BloomFilterCommandType.MEXISTS, new BooleanListOutput<>(codec), args);
    }

    public Command<K,V,String> bfReserve(K key, BfConfig config){
        CommandArgs<K,V> args = args(key);
        config.build(args);

        return createCommand(BloomFilterCommandType.RESERVE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,Boolean> cfAdd(K key, V item){
        CommandArgs<K,V> args = args(key);
        args.addValue(item);
        return createCommand(CuckooFilterCommandType.ADD, new BooleanOutput<>(codec), args);
    }

    public Command<K,V,Boolean> cfAddNx(K key, V item){
        CommandArgs<K,V> args = args(key);
        args.addValue(item);
        return createCommand(CuckooFilterCommandType.ADDNX, new BooleanOutput<>(codec), args);
    }

    public Command<K,V,Long> cfCount(K key, V item){
        CommandArgs<K,V> args = args(key);
        args.addValue(item);
        return createCommand(CuckooFilterCommandType.COUNT, new IntegerOutput<>(codec), args);
    }

    public Command<K,V,Boolean> cfDel(K key, V item){
        CommandArgs<K,V> args = args(key);
        args.addValue(item);
        return createCommand(CuckooFilterCommandType.DEL, new BooleanOutput<>(codec), args);
    }

    public Command<K,V,Boolean> cfExists(K key, V item){
        CommandArgs<K,V> args = args(key);
        args.addValue(item);
        return createCommand(CuckooFilterCommandType.EXISTS, new BooleanOutput<>(codec), args);
    }

    public Command<K,V,CfInfo> cfInfo(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(CuckooFilterCommandType.INFO, new CfInfoOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> cfInsert(K key, V[] items){
        CommandArgs<K,V> args = args(key);
        args.add("ITEMS");
        args.addValues(items);
        return createCommand(CuckooFilterCommandType.INSERT, new IntegerListOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> cfInsert(K key, V[] items, CfInsertOptions options){
        CommandArgs<K,V> args = args(key);
        options.build(args);
        args.add("ITEMS");
        args.addValues(items);
        return createCommand(CuckooFilterCommandType.INSERT, new IntegerListOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> cfInsertNx(K key, V[] items){
        CommandArgs<K,V> args = args(key);
        args.add("ITEMS");
        args.addValues(items);
        return createCommand(CuckooFilterCommandType.INSERTNX, new IntegerListOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> cfInsertNx(K key, V[] items, CfInsertOptions options){
        CommandArgs<K,V> args = args(key);
        options.build(args);
        args.add("ITEMS");
        args.addValues(items);
        return createCommand(CuckooFilterCommandType.INSERTNX, new IntegerListOutput<>(codec), args);
    }

    public Command<K,V,List<Boolean>> cfMExists(K key, V[] items){
        CommandArgs<K,V> args = args(key);
        args.addValues(items);
        return createCommand(CuckooFilterCommandType.MEXISTS, new BooleanListOutput<>(codec), args);
    }

    public Command<K,V,String> cfReserve(K key, Long capacity){
        CommandArgs<K,V> args = args(key);
        args.add(capacity);
        return createCommand(CuckooFilterCommandType.RESERVE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,String> cfReserve(K key, CfReserveOptions options){
        CommandArgs<K,V> args = args(key);
        options.build(args);
        return createCommand(CuckooFilterCommandType.RESERVE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,Long> cmsIncrBy(K key, V item, long increment){
        CommandArgs<K,V> args = args(key);
        args.addValue(item);
        args.add(increment);
        return createCommand(CountMinSketchCommandType.INCRBY, new IntegerOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> cmsIncrBy(K key, Map<V,Long> increments){
        CommandArgs<K,V> args = args(key);

        for(Map.Entry<V,Long> entry : increments.entrySet()){
            args.addValue(entry.getKey());
            args.add(entry.getValue());
        }
        return createCommand(CountMinSketchCommandType.INCRBY, new IntegerListOutput<>(codec),args);
    }

    public Command<K,V,String> cmsInitByProb(K key, double error, double probability){
        CommandArgs<K,V> args = args(key);
        args.add(error);
        args.add(probability);
        return createCommand(CountMinSketchCommandType.INITBYPROB, new StatusOutput<>(codec), args);
    }

    public Command<K,V,String> cmsInitByDim(K key, long width, long depth){
        CommandArgs<K,V> args = args(key);
        args.add(width);
        args.add(depth);
        return createCommand(CountMinSketchCommandType.INITBYDIM, new StatusOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> cmsQuery(K key, V... items){
        CommandArgs<K,V> args = args(key);
        args.addValues(items);
        return createCommand(CountMinSketchCommandType.QUERY, new IntegerListOutput<>(codec), args);
    }

    public Command<K,V, String> cmsMerge(K desKey, K... keys){
        CommandArgs<K,V> args = args(desKey);
        args.add(keys.length);
        args.addKeys(keys);
        return createCommand(CountMinSketchCommandType.MERGE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,String> cmsMerge(K destKey, Map<K,Long> keyWeightMap){
        CommandArgs<K,V> args = args(destKey);
        args.add(keyWeightMap.size());
        args.addKeys(keyWeightMap.keySet());
        args.add("WEIGHTS");
        for(Long weight : keyWeightMap.values()){
            args.add(weight);
        }

        return createCommand(CountMinSketchCommandType.MERGE, new StatusOutput<>(codec), args);
    }

    public Command<K,V, CmsInfo> cmsInfo(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(CountMinSketchCommandType.INFO, new CmsInfoOutput<>(codec), args);
    }

    public Command<K,V,List<Value<V>>> topKAdd(K key, V... items){
        CommandArgs<K,V> args = args(key);
        args.addValues(items);
        return createCommand(TopKCommandType.ADD, new ValueValueListOutput<>(codec), args);
    }

    public Command<K,V,List<Value<V>>> topKIncrBy(K key, Map<V,Long> increments){
        CommandArgs<K,V> args = args(key);
        for(Map.Entry<V,Long> entry : increments.entrySet()){
            args.addValue(entry.getKey());
            args.add(entry.getValue());
        }

        return createCommand(TopKCommandType.ADD, new ValueValueListOutput<>(codec), args);
    }

    public Command<K,V,TopKInfo> topKInfo(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(TopKCommandType.INFO, new TopKInfoOutput<>(codec), args);
    }

    public Command<K,V,List<String>> topKList(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(TopKCommandType.LIST, new StringListOutput<>(codec), args);
    }

    public Command<K,V,List<KeyValue<String, Long>>> topKListWithScores(K key){
        CommandArgs<K,V> args = args(key);
        args.add("WITHCOUNT");
        return createCommand(TopKCommandType.LIST, new TopKListWithScoresOutput<>(codec), args);
    }

    public Command<K,V,List<Boolean>> topKQuery(K key, V... items){
        CommandArgs<K,V> args = args(key);
        args.addValues(items);
        return createCommand(TopKCommandType.QUERY, new BooleanListOutput<>(codec), args);
    }

    public Command<K,V,String> topKReserve(K key, long k){
        CommandArgs<K,V> args = args(key);
        args.add(k);
        return createCommand(TopKCommandType.RESERVE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,String> topKReserve(K key, long k, long width, long depth, double decay){
        CommandArgs<K,V> args = args(key);
        args.add(k);
        args.add(width);
        args.add(depth);
        args.add(decay);
        return createCommand(TopKCommandType.RESERVE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,String> tDigestAdd(K key, double... values){
        CommandArgs<K,V> args = args(key);
        for(double val : values){
            args.add(val);
        }

        return createCommand(TDigestCommandType.ADD, new StatusOutput<>(codec), args);
    }

    public Command<K,V,List<Double>> tDigestByRank(K key, long... ranks){
        CommandArgs<K,V> args = args(key);
        for(long rank : ranks){
            args.add(rank);
        }

        return createCommand(TDigestCommandType.BYRANK, new DoubleListOutput<>(codec), args);
    }

    public Command<K,V,List<Double>> tDigestByRevRank(K key, long... ranks){
        CommandArgs<K,V> args = args(key);
        for(long rank : ranks){
            args.add(rank);
        }

        return createCommand(TDigestCommandType.BYREVRANK, new DoubleListOutput<>(codec), args);
    }

    public Command<K,V,List<Double>> tDigestCdf(K key, double... values){
        CommandArgs<K,V> args = args(key);
        for (double value : values){
            args.add(value);
        }

        return createCommand(TDigestCommandType.CDF, new DoubleListOutput<>(codec),args);
    }

    public Command<K,V,String> tDigestCreate(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(TDigestCommandType.CREATE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,String> tDigestCreate(K key, long compression){
        CommandArgs<K,V> args = args(key);
        args.add("COMPRESSION");
        args.add(compression);
        return createCommand(TDigestCommandType.CREATE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,TDigestInfo> tDigestInfo(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(TDigestCommandType.INFO, new TDigestInfoOutput<>(codec), args);
    }

    public Command<K,V,Double> tDigestMax(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(TDigestCommandType.MAX, new DoubleOutput<>(codec), args);
    }

    public Command<K,V,String> tDigestMerge(K destinationKey, K... sourceKeys){
        CommandArgs<K,V> args = args(destinationKey);
        args.addKeys(sourceKeys);
        return createCommand(TDigestCommandType.MERGE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,String> tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys){
        CommandArgs<K,V> args = args(destinationKey);
        args.addKeys(sourceKeys);
        options.build(args);
        return createCommand(TDigestCommandType.MERGE, new StatusOutput<>(codec), args);
    }

    public Command<K,V,Double> tDigestMin(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(TDigestCommandType.MIN, new DoubleOutput<>(codec), args);
    }

    public Command<K,V,List<Double>> tDigestQuantile(K key, double... quantiles){
        CommandArgs<K,V> args = args(key);
        for(double quantile : quantiles){
            args.add(quantile);
        }

        return createCommand(TDigestCommandType.QUANTILE, new DoubleListOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> tDigestRank(K key, double... values){
        CommandArgs<K,V> args = args(key);
        for(double value: values){
            args.add(value);
        }

        return createCommand(TDigestCommandType.RANK, new IntegerListOutput<>(codec), args);
    }

    public Command<K,V,String> tDigestReset(K key){
        CommandArgs<K,V> args = args(key);
        return createCommand(TDigestCommandType.RESET, new StatusOutput<>(codec), args);
    }

    public Command<K,V,List<Long>> tDigestRevRank(K key, double... values){
        CommandArgs<K,V> args = args(key);
        for(double value: values){
            args.add(value);
        }

        return createCommand(TDigestCommandType.REVRANK, new IntegerListOutput<>(codec), args);
    }

    public Command<K,V,Double> tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile){
        CommandArgs<K,V> args = args(key);
        args.add(lowCutQuantile);
        args.add(highCutQuantile);
        return createCommand(TDigestCommandType.TRIMMED_MEAN, new DoubleOutput<>(codec), args);
    }
}
