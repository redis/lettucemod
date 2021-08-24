package com.redis.lettucemod.gears;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

public class RedisGearsUtils {

    public static String toString(InputStream inputStream, Charset charset) {
        return toString(new InputStreamReader(inputStream, charset));
    }

    public static String toString(InputStreamReader reader) {
        return new BufferedReader(reader).lines().collect(Collectors.joining(System.lineSeparator()));
    }

    public static String toString(InputStream inputStream) {
        return toString(new InputStreamReader(inputStream));
    }

}
