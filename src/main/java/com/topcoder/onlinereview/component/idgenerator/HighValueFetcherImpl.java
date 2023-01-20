package com.topcoder.onlinereview.component.idgenerator;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.topcoder.or.util.DBAccessor;

import java.util.List;
import java.util.Map;

import static com.topcoder.or.util.Helper.getLong;

/**
 * High value fetcher implementation.
 *
 * @version 1.0
 * @author Timur Zambalayev
 */
final class HighValueFetcherImpl implements HighValueFetcher {

    private final DBAccessor dbAccessor;
    private final JdbcTemplate jdbcTemplate;
    private final String highValueColumnName;
    private final long maxHi;
    private final boolean autoInit;
    private final String selectSql;
    private final String updateSql;
    private final String insertSql;

    /**
     * Creates an instance of this class.
     *
     * @param tableName           table name.
     * @param userDefColumnName   userDef column name.
     * @param highValueColumnName high value column name.
     * @param maxHi               the maximum high value.
     * @param autoInit            if there's auto initialization
     */
    HighValueFetcherImpl(DBAccessor dbAccessor, JdbcTemplate jdbcTemplate, String tableName, String userDefColumnName,
            String highValueColumnName, long maxHi, boolean autoInit) {
        this.dbAccessor = dbAccessor;
        this.jdbcTemplate = jdbcTemplate;
        this.highValueColumnName = highValueColumnName;
        this.maxHi = maxHi;
        this.autoInit = autoInit;
        selectSql = "SELECT " + highValueColumnName + " FROM " + tableName + " WHERE " + userDefColumnName + "=?";
        updateSql = "UPDATE " + tableName + " SET " + highValueColumnName + "=" + highValueColumnName + "+1 WHERE "
                + userDefColumnName + "=?";
        insertSql = "INSERT INTO " + tableName + " (" + userDefColumnName + ", " + highValueColumnName
                + ") VALUES (?, 0)";
    }

    public long nextHighValue(String tableId) {
        setHighValue(tableId);
        long id = getCurrentHighValue(tableId);
        testId(id);
        return id;
    }

    private void setHighValue(String tableId) throws DataAccessException {
        int rowCount = dbAccessor.executeUpdate(jdbcTemplate, updateSql, tableId);
        if (rowCount != 1) {
            if (rowCount != 0) {
                throw new RuntimeException("rowCount=" + rowCount);
            }
            if (!autoInit) {
                throw new RuntimeException("no such row in the id generation table, tableId=" + tableId);
            }
            insertZero(tableId);
        }
    }

    private long getCurrentHighValue(String tableId) throws DataAccessException {
        List<Map<String, Object>> resultSet = dbAccessor.executeQuery(jdbcTemplate, selectSql, tableId);
        if (resultSet.isEmpty()) {
            throw new RuntimeException("no such row in the id generation table, tableId=" + tableId);
        }
        if (resultSet.size() > 1) {
            throw new RuntimeException("more than one row in the id generation table, tableId=" + tableId);
        }
        return getLong(resultSet.get(0), highValueColumnName);
    }

    private void insertZero(String tableId) throws DataAccessException {
        int rowCount = dbAccessor.executeUpdate(jdbcTemplate, insertSql, tableId);
        if (rowCount != 1) {
            throw new RuntimeException("rowCount=" + rowCount);
        }
    }

    private void testId(long id) {
        if (id >= maxHi || id < 0) {
            throw new IllegalStateException("idValue is out of range, idValue=" + id + ", maxHi=" + maxHi);
        }
    }

}
