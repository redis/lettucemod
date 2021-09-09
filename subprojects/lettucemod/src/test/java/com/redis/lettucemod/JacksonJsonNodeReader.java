package com.redis.lettucemod;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.json.JsonObjectReader;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of {@link JsonObjectReader} based on
 * <a href="https://github.com/FasterXML/jackson">Jackson</a>.
 *
 */
public class JacksonJsonNodeReader implements JsonObjectReader<JsonNode> {

    private JsonParser jsonParser;

    private ObjectMapper mapper;

    private InputStream inputStream;

    /**
     * Create a new {@link JacksonJsonNodeReader} instance.
     */
    public JacksonJsonNodeReader() {
        this(new ObjectMapper());
    }

    public JacksonJsonNodeReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Set the object mapper to use to map Json objects to domain objects.
     *
     * @param mapper the object mapper to use
     */
    public void setMapper(ObjectMapper mapper) {
        Assert.notNull(mapper, "The mapper must not be null");
        this.mapper = mapper;
    }

    @Override
    public void open(Resource resource) throws Exception {
        Assert.notNull(resource, "The resource must not be null");
        this.inputStream = resource.getInputStream();
        this.jsonParser = this.mapper.getFactory().createParser(this.inputStream);
        Assert.state(this.jsonParser.nextToken() == JsonToken.START_ARRAY,
                "The Json input stream must start with an array of Json objects");
    }

    @Nullable
    @Override
    public JsonNode read() {
        try {
            if (this.jsonParser.nextToken() == JsonToken.START_OBJECT) {
                return this.mapper.readTree(this.jsonParser);
            }
        } catch (IOException e) {
            throw new ParseException("Unable to read next JSON object", e);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        this.inputStream.close();
        this.jsonParser.close();
    }

}
