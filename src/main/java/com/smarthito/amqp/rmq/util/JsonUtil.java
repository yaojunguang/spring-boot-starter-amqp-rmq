package com.smarthito.amqp.rmq.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * JSON 工具封装
 *
 * @author yaojunguang
 */

@Slf4j
public class JsonUtil {

    private static final JsonUtil INSTANCE = new JsonUtil();

    public final ObjectMapper mapper = new ObjectMapper();

    private JsonUtil() {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
    }

    public static JsonUtil getInstance() {
        return INSTANCE;
    }

    //#region static 调用方法，模仿fast json使用

    public static JsonNode parseJsonNode(String jsonStr) {
        return getInstance().string2JsonNode(jsonStr);
    }

    public static <T> T parseObject(String json, Class<T> valueType) {
        return getInstance().string2Object(json, valueType);
    }

    public static <T> T parseObject(JsonNode jsonNode, Class<T> valueType) {
        return getInstance().jsonNode2Object(jsonNode, valueType);
    }

    public static <T> T parseObject(String json, TypeReference<T> typeReference) {
        return getInstance().deserialize(json, typeReference);
    }

    public static String toJsonString(Object value) {
        return getInstance().object2String(value, false);
    }

    public static String toJsonString(Object value, boolean pretty) {
        return getInstance().object2String(value, pretty);
    }

    //#endregion


    public <T> T deserialize(Map<String, String> map, Class<T> valueType) throws JsonProcessingException {
        return mapper.convertValue(map, valueType);
    }

    public <T> T deserialize(String json, Class<T> valueType) {
        return string2Object(json, valueType);
    }

    public <T> T deserializeBytes(byte[] val, Class<T> valueType) {
        T obj = null;
        try {
            obj = mapper.readValue(val, valueType);
        } catch (Exception e) {
            String tmp = "";
            tmp = new String(val, StandardCharsets.UTF_8);
            log.error(
                    String.format("JsonUtils.deserializeBytes json:%s classType:%s", tmp, valueType.getName()), e);

        }
        return obj;
    }

    public <T> T string2Object(String json, Class<T> valueType) {
        T obj = null;
        try {
            obj = mapper.readValue(json, valueType);
        } catch (IOException e) {
            log.error(
                    String.format("JsonUtils.string2Object json:%s classType:%s", json, valueType.getName()), e);

        }
        return obj;
    }

    public <T> T jsonNode2Object(JsonNode jsonNode, Class<T> valueType) {
        T obj = null;
        try {
            obj = mapper.treeToValue(jsonNode, valueType);
        } catch (IOException e) {
            log.error(
                    String.format("JsonUtils.string2Object json:%s classType:%s", jsonNode, valueType.getName()), e);

        }
        return obj;
    }

    public <T> T deserialize(String json, TypeReference<T> typeReference) {
        T obj = null;
        try {

            obj = mapper.readValue(json, typeReference);
        } catch (IOException e) {
            log.error(

                    String.format("JsonUtils.string2Object json:%s typeReference:%s", json,
                            typeReference.getType().getTypeName()),
                    e);

        }
        return obj;
    }

    public String serialize(Object value) {
        return object2String(value);
    }

    public <T> T string2Object(String json, Class<T> valueType, Boolean allowUnquotedControlChars) {

        T obj = null;
        try {
            if (allowUnquotedControlChars) {
                mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
                mapper.configure(Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
            }
            obj = mapper.readValue(json, valueType);
        } catch (IOException e) {
            log.error(String.format("JsonUtils.string2Object json:%s classType:%s", json, valueType.getName()), e);
        }

        return obj;
    }

    public String object2String(Object value) {
        return object2String(value, false);
    }

    public String object2String(Object value, boolean pretty) {
        String result = null;
        try {
            if (pretty) {
                result = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value);
            } else {
                result = mapper.writeValueAsString(value);
            }
        } catch (JsonProcessingException e) {
            log.error(String.format("JsonUtils.serialize class:%s", value.getClass().getName()), e);
        }

        return result;
    }

    /**
     * 字符串转化为JsonNode
     *
     * @param jsonStr 字符串
     * @return 结果
     */
    public JsonNode string2JsonNode(String jsonStr) {
        try {
            return mapper.readTree(jsonStr);
        } catch (IOException e) {
            log.error("conver to jsonNode error={}", e.getMessage());

        }
        return null;
    }

    public <T> T convertObject(Object map, Class<T> valueType) {
        return mapper.convertValue(map, valueType);
    }

    public <T> T convertObject(Object map, TypeReference<T> valueType) {
        return mapper.convertValue(map, valueType);
    }
}
