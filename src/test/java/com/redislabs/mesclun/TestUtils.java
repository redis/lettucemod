package com.redislabs.mesclun;

import com.redislabs.mesclun.search.Field;
import com.redislabs.mesclun.search.IndexInfo;
import com.redislabs.mesclun.search.RediSearchUtils;
import com.redislabs.mesclun.search.SearchResults;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("unchecked")
public class TestUtils extends AbstractSearchTest {

    @Test
    public void ftInfo() throws IOException, ExecutionException, InterruptedException {
        createBeerIndex();
        List<Object> infoList = async.ftInfo(INDEX).get();
        IndexInfo<String, String> info = RediSearchUtils.getInfo(infoList);
        Assertions.assertEquals(2348, info.getNumDocs());
        List<Field<String, String>> fields = info.getFields();
        Field.Text<String, String> nameField = (Field.Text<String, String>) fields.get(0);
        Assertions.assertEquals(NAME, nameField.getName());
        Assertions.assertFalse(nameField.isNoIndex());
        Assertions.assertFalse(nameField.isNoStem());
        Assertions.assertFalse(nameField.isSortable());
        Field.Tag<String, String> styleField = (Field.Tag<String, String>) fields.get(1);
        Assertions.assertEquals(STYLE, styleField.getName());
        Assertions.assertTrue(styleField.isSortable());
        Assertions.assertEquals(",", styleField.getSeparator());
    }

    @Test
    public void escapeTag() throws ExecutionException, InterruptedException {
        String index = "escapeTagTestIdx";
        String idField = "id";
        async.create(index, Field.tag(idField).build()).get();
        Map<String, String> doc1 = new HashMap<>();
        doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
        async.hmset("doc1", doc1).get();
        SearchResults<String, String> results = async.search(index, "@id:{" + RediSearchUtils.escapeTag("User1#test.org") + "}").get();
        Assertions.assertEquals(1, results.size());
    }
}
