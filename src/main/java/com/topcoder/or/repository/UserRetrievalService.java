package com.topcoder.or.repository;

import java.util.ArrayList;
import java.util.List;

import com.topcoder.onlinereview.grpc.userretrieval.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;

public class UserRetrievalService extends UserRetrievalServiceGrpc.UserRetrievalServiceImplBase {
    private final DBAccessor dbAccessor;

    public UserRetrievalService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void getUsersByUserIds(UserIdsRequest request, StreamObserver<GetUsersResponse> responseObserver) {
        List<ExternalUserProto> result = getUsers(validateAndGetUserIdCondition(request),
                request.getIdsList().toArray());
        responseObserver.onNext(GetUsersResponse.newBuilder().addAllExternalUsers(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUsersByHandles(HandlesRequest request, StreamObserver<GetUsersResponse> responseObserver) {
        List<ExternalUserProto> result = getUsers(validateAndGetHandlesCondition(request),
                request.getHandlesList().toArray());
        responseObserver.onNext(GetUsersResponse.newBuilder().addAllExternalUsers(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUsersByLowerHandles(HandlesRequest request, StreamObserver<GetUsersResponse> responseObserver) {
        List<ExternalUserProto> result = getUsers(validateAndGetLowerHandlesCondition(request),
                request.getHandlesList().stream().map(String::toLowerCase).toArray());
        responseObserver.onNext(GetUsersResponse.newBuilder().addAllExternalUsers(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUsersByName(NameRequest request, StreamObserver<GetUsersResponse> responseObserver) {
        List<String> names = new ArrayList<>();
        names.add(request.getFirstName());
        names.add(request.getLastName());
        List<ExternalUserProto> result = getUsers(validateAndGetNameCondition(request), names.toArray());
        responseObserver.onNext(GetUsersResponse.newBuilder().addAllExternalUsers(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAlternativeEmailsByUserIds(UserIdsRequest request,
            StreamObserver<GetAlternativeEmailsResponse> responseObserver) {
        List<EmailProto> result = getAlternativeEmails(validateAndGetUserIdCondition(request),
                request.getIdsList().toArray());
        responseObserver.onNext(GetAlternativeEmailsResponse.newBuilder().addAllEmails(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAlternativeEmailsByHandles(HandlesRequest request,
            StreamObserver<GetAlternativeEmailsResponse> responseObserver) {
        List<EmailProto> result = getAlternativeEmails(validateAndGetHandlesCondition(request),
                request.getHandlesList().toArray());
        responseObserver.onNext(GetAlternativeEmailsResponse.newBuilder().addAllEmails(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAlternativeEmailsByLowerHandles(HandlesRequest request,
            StreamObserver<GetAlternativeEmailsResponse> responseObserver) {
        List<EmailProto> result = getAlternativeEmails(validateAndGetLowerHandlesCondition(request),
                request.getHandlesList().stream().map(String::toLowerCase).toArray());
        responseObserver.onNext(GetAlternativeEmailsResponse.newBuilder().addAllEmails(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAlternativeEmailsByName(NameRequest request,
            StreamObserver<GetAlternativeEmailsResponse> responseObserver) {
        List<String> names = new ArrayList<>();
        names.add(request.getFirstName());
        names.add(request.getLastName());
        List<EmailProto> result = getAlternativeEmails(validateAndGetNameCondition(request), names.toArray());
        responseObserver.onNext(GetAlternativeEmailsResponse.newBuilder().addAllEmails(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUserRatingsByUserIds(UserIdsRequest request,
            StreamObserver<GetUserRatingsResponse> responseObserver) {
        List<UserRatingProto> result = getUserRatings(validateAndGetUserIdCondition(request),
                request.getIdsList().toArray());
        responseObserver.onNext(GetUserRatingsResponse.newBuilder().addAllUserRatings(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUserRatingsByHandles(HandlesRequest request,
            StreamObserver<GetUserRatingsResponse> responseObserver) {
        List<UserRatingProto> result = getUserRatings(validateAndGetHandlesCondition(request),
                request.getHandlesList().toArray());
        responseObserver.onNext(GetUserRatingsResponse.newBuilder().addAllUserRatings(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUserRatingsByLowerHandles(HandlesRequest request,
            StreamObserver<GetUserRatingsResponse> responseObserver) {
        List<UserRatingProto> result = getUserRatings(validateAndGetLowerHandlesCondition(request),
                request.getHandlesList().stream().map(String::toLowerCase).toArray());
        responseObserver.onNext(GetUserRatingsResponse.newBuilder().addAllUserRatings(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUserRatingsByName(NameRequest request,
            StreamObserver<GetUserRatingsResponse> responseObserver) {
        List<String> names = new ArrayList<>();
        names.add(request.getFirstName());
        names.add(request.getLastName());
        List<UserRatingProto> result = getUserRatings(validateAndGetNameCondition(request), names.toArray());
        responseObserver.onNext(GetUserRatingsResponse.newBuilder().addAllUserRatings(result).build());
        responseObserver.onCompleted();
    }

    private String validateAndGetUserIdCondition(UserIdsRequest request) {
        validateUserIdsRequest(request);
        String condition = """
                u.user_id in (%s)
                """;
        String inSql = Helper.getInClause(request.getIdsCount());
        return condition.formatted(inSql);
    }

    private String validateAndGetHandlesCondition(HandlesRequest request) {
        validateHandlesRequest(request);
        String condition = """
                u.handle in (%s)
                """;
        String inSql = Helper.getInClause(request.getHandlesCount());
        return condition.formatted(inSql);
    }

    private String validateAndGetLowerHandlesCondition(HandlesRequest request) {
        validateHandlesRequest(request);
        String condition = """
                u.handle_lower in (%s)
                """;
        String inSql = Helper.getInClause(request.getHandlesCount());
        return condition.formatted(inSql);
    }

    private String validateAndGetNameCondition(NameRequest request) {
        validateNameRequest(request);
        String condition = """
                u.first_name like ? and u.last_name like ?
                """;
        return condition;
    }

    private List<ExternalUserProto> getUsers(String condition, Object[] parameters) {
        String sql = """
                SELECT u.user_id, first_name, last_name, handle, address
                FROM user u, email
                WHERE u.user_id = email.user_id AND email.primary_ind = 1 AND
                """ + condition;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            ExternalUserProto.Builder builder = ExternalUserProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setUserId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setFirstName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setLastName);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setHandle);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setAddress);
            return builder.build();
        }, parameters);
    }

    private List<EmailProto> getAlternativeEmails(String condition, Object[] parameters) {
        String sql = """
                SELECT u.user_id, address
                FROM user u, email
                WHERE u.user_id = email.user_id AND email.primary_ind = 0 AND
                """ + condition;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            EmailProto.Builder builder = EmailProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setUserId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setAddress);
            return builder.build();
        }, parameters);
    }

    private List<UserRatingProto> getUserRatings(String condition, Object[] parameters) {
        String sql = """
                SELECT u.user_id id, r.rating rating, r.phase_id phaseId, vol volatility, num_ratings numRatings, ur.rating reliability
                FROM user u, user_rating r,
                OUTER user_reliability ur
                WHERE u.user_id = r.user_id AND u.user_id = ur.user_id AND r.phase_id = ur.phase_id AND
                """
                + condition;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            UserRatingProto.Builder builder = UserRatingProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setUserId);
            ResultSetHelper.applyResultSetInt(rs, 2, builder::setRating);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setPhaseId);
            ResultSetHelper.applyResultSetInt(rs, 4, builder::setVol);
            ResultSetHelper.applyResultSetInt(rs, 5, builder::setNumRatings);
            ResultSetHelper.applyResultSetDouble(rs, 6, builder::setReliability);
            return builder.build();
        }, parameters);
    }

    private void validateUserIdsRequest(UserIdsRequest request) {
        Helper.assertObjectNotEmpty(request::getIdsCount, "user_ids");
    }

    private void validateHandlesRequest(HandlesRequest request) {
        Helper.assertObjectNotEmpty(request::getHandlesCount, "handles");
    }

    private void validateNameRequest(NameRequest request) {
        Helper.assertObjectNotNull(request::hasFirstName, "first_name");
        Helper.assertObjectNotNull(request::hasLastName, "last_name");
    }
}
