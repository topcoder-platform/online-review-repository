package com.topcoder.or.util;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

@Component
public class DBAccessor {
    private final JdbcTemplate jdbcTemplate;
    private final Logger logger;

    @Autowired
    public DBAccessor(JdbcTemplate jdbcTemplate, Logger logger) {
        this.jdbcTemplate = jdbcTemplate;
        this.logger = logger;
    }

    /**
     * Execute query operation.
     *
     * @param query            The query clause, in format "from ... where ..."
     * @param params           The parameters to bind to query, may be null
     * @param returningColumns The columns to be returned
     * @return query result
     * @throws DataAccessException exception
     */
    public List<String[]> executeQuery(String query, String[] params, String[] returningColumns)
            throws DataAccessException {
        String sql = "SELECT " + String.join(",", returningColumns) + " " + query;

        this.logger.info("executeQuery: " + sql);
        return jdbcTemplate.query(sql, (rs, _rowNum) -> {
            String[] rowResult = new String[returningColumns.length];

            for (int idx = 0; idx < returningColumns.length; idx++) {
                Object value = rs.getObject(idx + 1);
                rowResult[idx] = value == null ? null : value.toString();
            }
            return rowResult;
        }, (Object[]) params);
    }

    /**
     * Execute update operation.
     *
     * @param query  The query clause
     * @param args The parameters to bind to query, may be null
     * @return the number of rows affected
     * @throws DataAccessException exception
     */
    public int executeUpdate(String query, @Nullable Object ...args) throws DataAccessException {
        this.logger.info("executeUpdate: " + query + " with params: " + Arrays.toString(args));
        return jdbcTemplate.update(query, args);
    }
}
