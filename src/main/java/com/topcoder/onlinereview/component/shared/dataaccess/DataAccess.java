package com.topcoder.onlinereview.component.shared.dataaccess;

import com.topcoder.or.util.DBAccessor;

import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * This bean processes a {@link RequestInt} and returns the data.
 *
 * @author Dave Pecora
 * @version $Revision$
 * @see RequestInt
 */
public class DataAccess {
  private DBAccessor dbAccessor;
  private JdbcTemplate jdbcTemplate;

  /**
   * Construtor that takes a data source to be used.
   *
   * @param jdbcTemplate
   */
  public DataAccess(DBAccessor dbAccessor, JdbcTemplate jdbcTemplate) {
    this.dbAccessor = dbAccessor;
    this.jdbcTemplate = jdbcTemplate;
  }

  /**
   * This method passes a query command request and a connection to the data
   * retriever and receives and passes on the results
   *
   * @param request A <tt>RequestInt</tt> request object containing a number of
   *                input property values.
   * @return A map of the query results, where the keys are strings of query names
   *         and the values are <tt>ResultSetContainer</tt> objects.
   * @throws Exception if there was an error encountered while retrieving the data
   *                   from the EJB.
   */
  public Map<String, List<Map<String, Object>>> getData(RequestInt request) throws DataAccessException {
    DataRetriever dr = getDataRetriever();
    return dr.executeCommand(request.getProperties());
  }

  protected DataRetriever getDataRetriever() {
    return new DataRetriever(dbAccessor, jdbcTemplate);
  }
}
