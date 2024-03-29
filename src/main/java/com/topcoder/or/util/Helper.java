package com.topcoder.or.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.Timestamp;
import com.topcoder.onlinereview.grpc.payment.proto.BigDecimalProto;

public final class Helper {

    /**
     * Check if the given object is null.
     *
     * @param verifier method to be used to verify object
     * @param name     the name to identify the object.
     * @throws IllegalArgumentException if the given object is null
     */
    public static void assertObjectNotNull(Supplier<Boolean> verifier, String name) {
        if (!verifier.get()) {
            throw new IllegalArgumentException("%s is required".formatted(name));
        }
    }

    /**
     * Check if the given object is null and positive.
     *
     * @param verifier method to be used to verify object
     * @param get      method to be used to get object
     * @param name     the name to identify the object.
     * @throws IllegalArgumentException if the given object is null
     */
    public static void assertObjectNotNullAndPositive(Supplier<Boolean> verifier, Supplier<Number> get, String name) {
        if (!verifier.get() || get.get().intValue() < 1) {
            throw new IllegalArgumentException("%s is required".formatted(name));
        }
    }

    /**
     * Check if the given object is not empty
     *
     * @param counter method to be used to count object
     * @param name    the name to identify the object.
     * @throws IllegalArgumentException if the given object is null
     */
    public static void assertObjectNotEmpty(Supplier<Integer> counter, String name) {
        if (counter.get() == 0) {
            throw new IllegalArgumentException("%s is required".formatted(name));
        }
    }

    public static <T> T extract(Supplier<Boolean> verifier, Supplier<T> extrator) {
        return verifier.get() ? extrator.get() : null;
    }

    public static Date extractDate(Supplier<Boolean> verifier, Supplier<Timestamp> extractor) {
        return verifier.get() ? new Date(extractor.get().getSeconds() * 1000) : null;
    }

    public static BigDecimal extractBigDecimal(Supplier<Boolean> verifier, Supplier<BigDecimalProto> extractor) {
        if (!verifier.get()) {
            return null;
        }
        BigDecimalProto serialized = extractor.get();
        return new BigDecimal(new BigInteger(serialized.getValue().toByteArray()), serialized.getScale(),
                new MathContext(serialized.getPrecision()));
    }

    public static Date convertDate(Timestamp date) {
        return new Date(date.getSeconds() * 1000);
    }

    public static java.sql.Timestamp convertTimestamp(Timestamp date) {
        return new java.sql.Timestamp(date.getSeconds() * 1000);
    }

    public static String getInClause(Integer count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    public static String buildNStatement(Integer count, String phrase, String delimiter) {
        return String.join(delimiter, Collections.nCopies(count, phrase));
    }

    private static <T> T getT(Map<String, Object> map, String key, Function<Object, T> function) {
        if (map.get(key) == null) {
            return null;
        }
        return function.apply(map.get(key));
    }

    public static Long getLong(Map<String, Object> map, String key) {
        return getT(map, key, v -> Long.parseLong(v.toString()));
    }

    public static Integer getInt(Map<String, Object> map, String key) {
        return getT(map, key, v -> Integer.parseInt(v.toString()));
    }

    public static Double getDouble(Map<String, Object> map, String key) {
        return getT(map, key, v -> Double.parseDouble(v.toString()));
    }

    public static Float getFloat(Map<String, Object> map, String key) {
        return getT(map, key, v -> Float.parseFloat(v.toString()));
    }

    public static Boolean getBoolean(Map<String, Object> map, String key) {
        return getT(
                map,
                key,
                v -> {
                    if (v instanceof Boolean) {
                        return (Boolean) v;
                    } else if (StringUtils.isNumeric(v.toString())) {
                        return Integer.parseInt(v.toString()) != 0;
                    } else {
                        return "true".equalsIgnoreCase(v.toString());
                    }
                });
    }

    public static String getString(Map<String, Object> map, String key) {
        return getT(
                map,
                key,
                v -> {
                    if (v instanceof byte[]) {
                        return new String((byte[]) v);
                    }
                    return v.toString();
                });
    }

    public static Date getDate(Map<String, Object> map, String key) {
        return getT(map, key, v -> (Date) v);
    }
}
