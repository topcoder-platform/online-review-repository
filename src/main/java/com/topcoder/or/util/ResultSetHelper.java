package com.topcoder.or.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;

public final class ResultSetHelper {

    public static void applyResultSetLong(ResultSet resultset, int index, Consumer<Long> setMethod)
            throws SQLException {
        long v = resultset.getLong(index);
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
}
