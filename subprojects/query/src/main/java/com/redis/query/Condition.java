package com.redis.query;

/**
 * Created by mnunberg on 2/23/18.
 *
 * Base node interface
 */
public interface Condition {

    /**
     * Create an intersect condition between the given condition and this condition.
     * Intersect confitions are true iff <b>all</b> of its children are true.
     * 
     * @return The intersect condition
     */
    Condition and(Condition condition);

    /**
     * Create a union condition between the given condition and this condition.
     * Union conditions are true iff <b>any</b> of its children are true.
     * 
     * @return The union condition
     */
    Condition or(Condition condition);

    String getQuery();

    Condition not();

    Condition optional();

}
