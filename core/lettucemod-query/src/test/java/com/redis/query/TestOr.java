package com.redis.query;

import com.redis.search.query.filter.Condition;
import com.redis.search.query.filter.Or;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestOr {
    Condition left;
    Condition right;

    @BeforeEach
    public void setup() {
        left = Mockito.mock(Condition.class);
        right = Mockito.mock(Condition.class);
    }

    @Test
    public void testOrCondition_whenLeftRightIsPresent_thenReturnQuery() {

        Mockito.when(left.getQuery()).thenReturn("@category:{italian}");
        Mockito.when(right.getQuery()).thenReturn("@priceForTwo:[1500 2000]");

        Or orCondition = new Or(left, right);
        String orQuery = orCondition.getQuery();

        Assertions.assertEquals("(@category:{italian})|(@priceForTwo:[1500 2000])", orQuery);
    }

    @Test
    public void testOrCondition_whenRightIsNullLeftIsPresent_thenReturnQuery() {

        Mockito.when(left.getQuery()).thenReturn("@category:{italian}");
        Mockito.when(right.getQuery()).thenReturn(" ");

        Or orCondition = new Or(left, right);
        String orQuery = orCondition.getQuery();

        Assertions.assertEquals("(@category:{italian})", orQuery);
    }

    @Test
    public void testOrCondition_whenRightIsEmptyLeftIsEmpty_thenReturnEmptyQuery() {

        Mockito.when(left.getQuery()).thenReturn("");
        Mockito.when(right.getQuery()).thenReturn("");

        Or orCondition = new Or(left, right);
        String orQuery = orCondition.getQuery();

        Assertions.assertEquals("", orQuery);
    }

    @Test
    public void testOrCondition_whenRightIsNullLeftIsNull_thenReturnEmptyQuery() {

        Mockito.when(left.getQuery()).thenReturn(null);
        Mockito.when(right.getQuery()).thenReturn(null);

        Or orCondition = new Or(left, right);
        String orQuery = orCondition.getQuery();

        Assertions.assertEquals("", orQuery);
    }
}
