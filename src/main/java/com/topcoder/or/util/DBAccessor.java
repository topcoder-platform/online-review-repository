package com.topcoder.or.util;

import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class DBAccessor {
    private final JdbcTemplate tcsJdbcTemplate;
    private final JdbcTemplate oltpJdbcTemplate;
    private final JdbcTemplate tcsDwJdbcTemplate;
    private final JdbcTemplate commonJdbcTemplate;
    private final Logger logger;

    public DBAccessor(JdbcTemplate tcsJdbcTemplate, JdbcTemplate oltpJdbcTemplate, JdbcTemplate tcsDwJdbcTemplate,
            JdbcTemplate commonJdbcTemplate, Logger logger) {
        this.tcsJdbcTemplate = tcsJdbcTemplate;
        this.oltpJdbcTemplate = oltpJdbcTemplate;
        this.tcsDwJdbcTemplate = tcsDwJdbcTemplate;
        this.commonJdbcTemplate = commonJdbcTemplate;
        this.logger = logger;
    }

    public JdbcTemplate getTcsJdbcTemplate() {
        return tcsJdbcTemplate;
    }

    public JdbcTemplate getOltpJdbcTemplate() {
        return oltpJdbcTemplate;
    }

    public JdbcTemplate getTcsDwJdbcTemplate() {
        return tcsDwJdbcTemplate;
    }

    public JdbcTemplate getCommonJdbcTemplate() {
        return commonJdbcTemplate;
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
        return executeQuery(tcsJdbcTemplate, query, mapper, args);
    }

    public <T> List<T> executeQuery(JdbcTemplate jdbcTemplate, String query, RowMapper<T> mapper,
            @Nullable Object... args) throws DataAccessException {
        logQuery("executeQuery", query, args);
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
        return executeUpdate(tcsJdbcTemplate, query, args);
    }

    public int executeUpdate(JdbcTemplate jdbcTemplate, String query, @Nullable Object... args)
            throws DataAccessException {
        logQuery("executeUpdate", query, args);
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
    public List<Map<String, Object>> executeQuery(String query, @Nullable Object... args) throws DataAccessException {
        return executeQuery(tcsJdbcTemplate, query, args);
    }

    public List<Map<String, Object>> executeQuery(JdbcTemplate jdbcTemplate, String query, @Nullable Object... args)
            throws DataAccessException {
        logQuery("executeQueryForList", query, args);
        return jdbcTemplate.queryForList(query, args);
    }

    public Number executeUpdateReturningKey(String query, PreparedStatementCreator psc) throws DataAccessException {
        return executeUpdateReturningKey(tcsJdbcTemplate, query, psc);
    }

    public Number executeUpdateReturningKey(JdbcTemplate jdbcTemplate, String query, PreparedStatementCreator psc)
            throws DataAccessException {
        logQuery("executeUpdate", query);
        GeneratedKeyHolder generatedKeyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(psc, generatedKeyHolder);
        return generatedKeyHolder.getKey();
    }

    public SqlRowSet queryForRowSet(JdbcTemplate jdbcTemplate, String sql) throws DataAccessException {
        logQuery("executeQueryForRowSet", sql);
        return jdbcTemplate.queryForRowSet(sql);
    }

    private void logQuery(String type, String query, @Nullable Object... args) {
        String sanitized = query.substring(0, Math.min(query.length(), 150)).replaceAll("\n", " ");
        if (args != null && args.length > 0) {
            logger.info(type + ": '{}' with params: {}", sanitized, Arrays.toString(args));
        } else {
            logger.info(type + ": '{}'", sanitized);
        }
    }
}
