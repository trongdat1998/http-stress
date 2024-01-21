/*
 ************************************
 * @项目名称: quote
 * @文件名称: JsonUtil
 * @Date 2018/06/08
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

@Slf4j
public class JsonUtil {

    private static final Gson gson = new GsonBuilder().create();

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }

    public static <T> T fromJson(String json, Type type) {
        return gson.fromJson(json, type);
    }

    public static String toJson(Object o) {
        return gson.toJson(o);
    }

    public static Map<String, Object> toMap(String json) {
        try {
            return gson.fromJson(json, new TypeToken<Map<String, Object>>() {
            }.getType());
        } catch (Exception e) {
            log.error("fromJson error", e);
        }
        return null;
    }

    private static final Gson DEFAULT_GSON = new GsonBuilder()
            .setLongSerializationPolicy(LongSerializationPolicy.STRING)
            .setExclusionStrategies(new JsonIgnoreExclusionStrategy())
            .registerTypeAdapter(Date.class, (JsonSerializer<Date>) (date, type, context) -> new JsonPrimitive(defaultDateFormat().format(date)))
            .registerTypeAdapter(Date.class, (JsonDeserializer<Date>) (date, type, context) -> {
                try {
                    return defaultDateFormat().parse(date.getAsString());
                } catch (ParseException e) {
                    throw new JsonParseException(e);
                }
            })
            .create();

    public static Gson defaultGson() {
        return DEFAULT_GSON;
    }

    private static class JsonIgnoreExclusionStrategy implements ExclusionStrategy {
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(JsonIgnore.class) != null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return clazz.getAnnotation(JsonIgnore.class) != null;
        }
    }

    private static DateFormat defaultDateFormat() {
        DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
        df.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        return df;
    }


}


