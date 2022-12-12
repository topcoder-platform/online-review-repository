package com.topcoder.or.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;

public final class ResultSetHelper {

    public static void applyResultSetLong(ResultSet resultset, int index, Consumer<Int64Value> setMethod)
            throws SQLException {
        long v = resultset.getLong(index);
        if (!resultset.wasNull()) {
            setMethod.accept(Int64Value.of(v));
        }
    }

    public static void applyResultSetBool(ResultSet resultset, int index, Consumer<BoolValue> setMethod)
            throws SQLException {
        boolean v = resultset.getBoolean(index);
        if (!resultset.wasNull()) {
            setMethod.accept(BoolValue.of(v));
        }
    }

    public static void applyResultSetString(ResultSet resultset, int index, Consumer<StringValue> setMethod)
            throws SQLException {
        String v = resultset.getString(index);
        if (v != null) {
            setMethod.accept(StringValue.of(v));
        }
    }

    public static void applyResultSetTimestamp(ResultSet resultset, int index, Consumer<Timestamp> setMethod)
            throws SQLException {
        java.sql.Timestamp v = resultset.getTimestamp(index);
        if (v != null) {
            setMethod.accept(Timestamp.newBuilder().setSeconds(v.toInstant().getEpochSecond()).build());
        }
    }
}
