package com.topcoder.onlinereview.component.idgenerator;

import org.springframework.dao.DataAccessException;

/**
 * Id generator interface.
 * 
 * @version 1.0
 * @author Timur Zambalayev
 */
interface IdGeneratorInterface {

    /**
     * Gets the next id.
     * 
     * @return the next id.
     * @throws DataAccessException if a db access error occurs.
     */
    long nextId() throws DataAccessException;

}
