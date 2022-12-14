package com.topcoder.or.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;

import com.google.protobuf.Timestamp;

public final class ResultSetHelper {

    public static void applyResultSetLong(ResultSet resultset, int index, Consumer<Long> setMethod)
            throws SQLException {
        long v = resultset.getLong(index);
        if (!resultset.wasNull()) {
            setMethod.accept(v);
        }
    }

    public static void applyResultSetLong(ResultSet resultset, String name, Consumer<Long> setMethod)
            throws SQLException {
        long v = resultset.getLong(name);
        if (!resultset.wasNull()) {
            setMethod.accept(v);
        }
    }

    public static void applyResultSetBool(ResultSet resultset, int index, Consumer<Boolean> setMethod)
            throws SQLException {
        boolean v = resultset.getBoolean(index);
        if (!resultset.wasNull()) {
            setMethod.accept(v);
        }
    }

    public static void applyResultSetBool(ResultSet resultset, String name, Consumer<Boolean> setMethod)
            throws SQLException {
        boolean v = resultset.getBoolean(name);
        if (!resultset.wasNull()) {
            setMethod.accept(v);
        }
    }

    public static void applyResultSetString(ResultSet resultset, int index, Consumer<String> setMethod)
            throws SQLException {
        String v = resultset.getString(index);
        if (v != null) {
            setMethod.accept(v);
        }
    }

    public static void applyResultSetString(ResultSet resultset, String name, Consumer<String> setMethod)
            throws SQLException {
        String v = resultset.getString(name);
        if (v != null) {
            setMethod.accept(v);
        }
    }

    public static void applyResultSetTimestamp(ResultSet resultset, int index, Consumer<Timestamp> setMethod)
            throws SQLException {
        java.sql.Timestamp v = resultset.getTimestamp(index);
        if (v != null) {
            setMethod.accept(Timestamp.newBuilder().setSeconds(v.toInstant().getEpochSecond()).build());
        }
    }

    public static void applyResultSetTimestamp(ResultSet resultset, String name, Consumer<Timestamp> setMethod)
            throws SQLException {
        java.sql.Timestamp v = resultset.getTimestamp(name);
        if (v != null) {
            setMethod.accept(Timestamp.newBuilder().setSeconds(v.toInstant().getEpochSecond()).build());
        }
    }
}
