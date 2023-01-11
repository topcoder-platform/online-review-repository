package com.topcoder.or.repository;

import java.util.List;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.grpc.reviewupload.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ReviewUploadService extends ReviewUploadServiceGrpc.ReviewUploadServiceImplBase {
    private final DBAccessor dbAccessor;

    public ReviewUploadService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void isProjectResultExists(ProjectUserRequest request, StreamObserver<ExistsProto> responseObserver) {
        validateProjectUserRequest(request);
        boolean result = checkEntityExists("PROJECT_RESULT", request.getProjectId(), request.getUserId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isComponentInquiryExists(ProjectUserRequest request, StreamObserver<ExistsProto> responseObserver) {
        validateProjectUserRequest(request);
        boolean result = checkEntityExists("component_inquiry", request.getProjectId(), request.getUserId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUserRating(ProjectUserRequest request, StreamObserver<RatingProto> responseObserver) {
        validateProjectUserRequest(request);
        String sql = """
                SELECT rating
                from user_rating
                where user_id = ? and phase_id = (select 111+project_category_id from project where project_id = ?)
                """;
        List<RatingProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            RatingProto.Builder builder = RatingProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setRating);
            return builder.build();
        }, request.getUserId(), request.getProjectId());
        responseObserver.onNext(result.isEmpty() ? RatingProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void createProjectResult(ProjectResultProto request, StreamObserver<CountProto> responseObserver) {
        validateProjectResultProto(request);
        String sql = """
                INSERT INTO project_result (project_id, user_id, rating_ind, valid_submission_ind, old_rating)
                values (?, ?, ?, ?, ?)
                """;
        Double oldRating = Helper.extract(request::hasOldRating, request::getOldRating);
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getUserId(),
                request.getRatingInd(), request.getValidSubmissionInd(), oldRating);
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createComponentInquiry(ComponentInquiryProto request, StreamObserver<CountProto> responseObserver) {
        validateComponentInquiryProto(request);
        String sql = """
                INSERT INTO component_inquiry (component_inquiry_id, component_id, user_id, project_id, phase, tc_user_id, agreed_to_terms, rating, version, create_time)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, current)
                """;
        Long phaseId = Helper.extract(request::hasPhase, request::getPhase);
        int affected = dbAccessor.executeUpdate(sql, request.getComponentInquiryId(), request.getComponentId(),
                request.getUserId(), request.getProjectId(), phaseId, request.getTcUserId(), request.getAgreedToTerms(),
                request.getRating(), request.getVersion());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNextId(Empty request, StreamObserver<NextIdProto> responseObserver) {
        String sql = """
                SELECT max(current_value) as cur_value FROM sequence_object WHERE name = 'main_sequence'
                """;
        List<Long> currentValues = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        });
        responseObserver.onNext(NextIdProto.newBuilder().setCurrentValue(currentValues.get(0)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateNextId(UpdateNextIdRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateNextIdRequest(request);
        String sql = """
                UPDATE sequence_object SET current_value = ? WHERE name = 'main_sequence' AND current_value = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getNewCurrentValue(), request.getOldCurrentValue());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    private boolean checkEntityExists(String tableName, long projectId, String userId) {
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM %s WHERE user_id = ? AND project_id = ?) THEN 1 ELSE 0 END FROM DUAL
                """
                .formatted(tableName);
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, userId, projectId).get(0);
    }

    private void validateProjectUserRequest(ProjectUserRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateProjectResultProto(ProjectResultProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasRatingInd, "rating_ind");
        Helper.assertObjectNotNull(request::hasValidSubmissionInd, "valid_submission_ind");
    }

    private void validateComponentInquiryProto(ComponentInquiryProto request) {
        Helper.assertObjectNotNull(request::hasComponentInquiryId, "component_inquiry_id");
        Helper.assertObjectNotNull(request::hasComponentId, "component_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasTcUserId, "tc_user_id");
        Helper.assertObjectNotNull(request::hasAgreedToTerms, "agreed_to_terms");
        Helper.assertObjectNotNull(request::hasRating, "rating");
        Helper.assertObjectNotNull(request::hasVersion, "version");
    }

    private void validateUpdateNextIdRequest(UpdateNextIdRequest request) {
        Helper.assertObjectNotNull(request::hasNewCurrentValue, "new_current_value");
        Helper.assertObjectNotNull(request::hasOldCurrentValue, "old_current_value");
    }
}
