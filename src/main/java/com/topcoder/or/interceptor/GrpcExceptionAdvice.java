package com.topcoder.or.interceptor;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;

@GrpcAdvice
public class GrpcExceptionAdvice {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @GrpcExceptionHandler
    public StatusRuntimeException handleError(IllegalArgumentException e) {
        logger.error(e.getLocalizedMessage(), e);
        return Status.INVALID_ARGUMENT.withDescription(e.getLocalizedMessage()).withCause(e).asRuntimeException();
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handleError(SQLException e) {
        logger.error(e.getLocalizedMessage(), e);
        return Status.INVALID_ARGUMENT.withDescription(e.getLocalizedMessage()).withCause(e).asRuntimeException();
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handleError(org.springframework.jdbc.BadSqlGrammarException e) {
        logger.error(e.getLocalizedMessage(), e);
        return Status.INVALID_ARGUMENT.withDescription(e.getLocalizedMessage()).withCause(e).asRuntimeException();
    }
}
