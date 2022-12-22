package com.topcoder.or.util;

import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class DBAccessor {
    private final JdbcTemplate jdbcTemplate;
    private final Logger logger;

    public DBAccessor(JdbcTemplate jdbcTemplate, Logger logger) {
        this.jdbcTemplate = jdbcTemplate;
        this.logger = logger;
    }

    /**
     * Execute query operation.
     *
     * @param <T>    This describes type of returning object
     * @param query  The complete query clause
     * @param params The parameters to bind to query, may be null
     * @param mapper {@link org.springframework.jdbc.core.RowMapper RowMapper}
     * @return Mapped query result
     * @throws DataAccessException exception
     */
    public <T> List<T> executeQuery(String query, RowMapper<T> mapper, @Nullable Object... args)
            throws DataAccessException {
        logger.info("executeQuery: " + query + " with params: " + Arrays.toString(args));
        return jdbcTemplate.query(query, mapper, args);
    }

    /**
     * Execute update operation.
     *
     * @param query The query clause
     * @param args  The parameters to bind to query, may be null
     * @return the number of rows affected
     * @throws DataAccessException exception
     */
    public int executeUpdate(String query, @Nullable Object... args) throws DataAccessException {
        logger.info("executeUpdate: " + query + " with params: " + Arrays.toString(args));
        return jdbcTemplate.update(query, args);
    }

    /**
     * Execute update operation.
     *
     * @param query The query clause
     * @param args  The parameters to bind to query, may be null
     * @return the result map list
     * @throws DataAccessException exception
     */
    public List<Map<String, Object>> executeQuery(String query, @Nullable Object... args) {
        logger.info("executeQuery: " + query + " with params: " + Arrays.toString(args));
        return jdbcTemplate.queryForList(query, args);
    }
}
