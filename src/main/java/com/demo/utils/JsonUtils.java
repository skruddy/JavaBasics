package com.demo.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonUtils {

    private final static ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static <T> String json(T body) {
        try {
            return objectMapper.writeValueAsString(body);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static <T> T readValue(String value, Class<T> valueType) {
        try {
            return objectMapper.readValue(value, valueType);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static <T> T readValue(String value, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(value, typeReference);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
