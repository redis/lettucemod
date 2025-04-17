package com.redis.query;

import com.redis.search.query.filter.Condition;
import com.redis.search.query.filter.OrList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestOrList {
    Condition cond1;
    Condition cond2;
    Condition cond3;

    @BeforeEach
    public void setup()
    {
        cond1= Mockito.mock(Condition.class);
        cond2= Mockito.mock(Condition.class);
        cond3= Mockito.mock(Condition.class);
    }

    @Test
    public void testOrCondition_whenConditionsArePresent_thenReturnQuery()
    {
        Mockito.when(cond1.getQuery()).thenReturn("@category:{italian}");
        Mockito.when(cond2.getQuery()).thenReturn("@price:[1000 2000]");
        Mockito.when(cond3.getQuery()).thenReturn("@rating:[4.0 5.0]");

        OrList orListCondition= new OrList(cond1,cond2,cond3);
        String orListQuery= orListCondition.getQuery();

        Assertions.assertEquals("((@category:{italian})|(@price:[1000 2000])|(@rating:[4.0 5.0]))",orListQuery);
    }

    @Test
    public void testOrCondition_whenConditionsAreEmpty_thenReturnEmptyQuery()
    {
        Mockito.when(cond1.getQuery()).thenReturn("");
        Mockito.when(cond2.getQuery()).thenReturn("");
        Mockito.when(cond3.getQuery()).thenReturn("");

        OrList orListCondition= new OrList(cond1,cond2,cond3);
        String orListQuery= orListCondition.getQuery();

        Assertions.assertEquals("(()|()|())",orListQuery);
    }

    @Test
    public void testOrCondition_whenConditionsAreNull_thenReturnEmptyQuery()
    {
        Mockito.when(cond1.getQuery()).thenReturn(null);
        Mockito.when(cond2.getQuery()).thenReturn(null);
        Mockito.when(cond3.getQuery()).thenReturn(null);

        OrList orListCondition= new OrList(cond1,cond2,cond3);
        String orListQuery= orListCondition.getQuery();

        Assertions.assertEquals("(()|()|())",orListQuery);
    }

    @Test
    public void testOrCondition_whenSingleConditionIsPresent_thenReturnQuery()
    {
        Mockito.when(cond1.getQuery()).thenReturn("@category:{italian}");

        OrList orListCondition= new OrList(cond1);
        String orListQuery= orListCondition.getQuery();

        Assertions.assertEquals("((@category:{italian}))",orListQuery);
    }
}
