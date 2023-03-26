package com.topcoder.onlinereview.component.idgenerator;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.topcoder.or.util.DBAccessor;

/**
 * Id generator implementation.
 *
 * @version 1.0
 * @author Timur Zambalayev
 */
final class IdGeneratorImpl implements IdGeneratorInterface {

    private final IdGeneratorInterface generator;

    /**
     * Creates an instance of this class.
     *
     * @param jdbcTemplate        jdbc template.
     * @param tableName           table name.
     * @param userDefColumnName   userDef column name.
     * @param highValueColumnName high value column name.
     * @param maxHi               the maximum high value.
     * @param maxLo               the maximum low value.
     * @param autoInit            if there's auto initialization
     */
    IdGeneratorImpl(DBAccessor dbAccessor, JdbcTemplate jdbcTemplate, String tableName, String userDefColumnName,
            String highValueColumnName, long maxHi, int maxLo, boolean autoInit, String tableId) {
        HighValueFetcherImpl fetcher = new HighValueFetcherImpl(dbAccessor, jdbcTemplate, tableName, userDefColumnName,
                highValueColumnName, maxHi, autoInit);
        generator = new HighLowIdGenerator(maxLo, fetcher, tableId);
    }

    public long nextId() throws DataAccessException {
        return generator.nextId();
    }

}
