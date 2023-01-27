package com.topcoder.or.repository;

import java.util.List;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.grpc.actionshelper.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ActionsHelperService extends ActionsHelperServiceGrpc.ActionsHelperServiceImplBase {
    private final DBAccessor dbAccessor;

    public ActionsHelperService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void getUserPreferenceValue(GetUserPreferenceValueRequest request,
            StreamObserver<GetUserPreferenceValueResponse> responseObserver) {
        validateGetUserPreferenceValueRequest(request);
        String sql = """
                select value
                from user_preference
                where user_id = ? and preference_id = ?
                """;
        List<String> result = dbAccessor.executeQuery(dbAccessor.getCommonJdbcTemplate(), sql, (rs, _i) -> {
            return rs.getString(1);
        }, request.getUserId(), request.getPreferenceId());
        responseObserver
                .onNext(result.isEmpty() || result.get(0) == null ? GetUserPreferenceValueResponse.getDefaultInstance()
                        : GetUserPreferenceValueResponse.newBuilder().setValue(result.get(0)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isProjectResultExists(IsProjectResultExistsRequest request,
            StreamObserver<IsProjectResultExistsResponse> responseObserver) {
        validateIsProjectResultExistsRequest(request);
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM PROJECT_RESULT WHERE user_id = ? and project_id = ?) THEN 1 ELSE 0 END FROM DUAL
                """;
        boolean result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, request.getUserId(), request.getProjectId()).get(0);
        responseObserver.onNext(IsProjectResultExistsResponse.newBuilder().setIsProjectResultExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isComponentInquiryExists(IsComponentInquiryExistsRequest request,
            StreamObserver<IsComponentInquiryExistsResponse> responseObserver) {
        validateIsComponentInquiryExistsRequest(request);
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM component_inquiry WHERE user_id = ? and project_id = ?) THEN 1 ELSE 0 END FROM DUAL
                """;
        boolean result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, request.getUserId(), request.getProjectId()).get(0);
        responseObserver
                .onNext(IsComponentInquiryExistsResponse.newBuilder().setIsComponentInquiryExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getRatings(GetRatingsRequest request, StreamObserver<GetRatingsResponse> responseObserver) {
        validateGetRatingsRequest(request);
        String sql = """
                SELECT rating, phase_id, (select project_category_id from project where project_id = ?) as project_category_id
                from user_rating
                where user_id = ?
                """;
        List<RatingProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            RatingProto.Builder builder = RatingProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setRating);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setProjectCategoryId);
            return builder.build();
        }, request.getProjectId(), request.getUserId());
        responseObserver.onNext(GetRatingsResponse.newBuilder().addAllRatings(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createProjectResult(CreateProjectResultRequest request, StreamObserver<CountProto> responseObserver) {
        validateCreateProjectResultRequest(request);
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
    public void createComponentInquiry(CreateComponentInquiryRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateCreateComponentInquiryRequest(request);
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
    public void getNextComponentInquiryId(Empty request, StreamObserver<NextIdProto> responseObserver) {
        String sql = """
                SELECT max(current_value) as seq_id FROM sequence_object WHERE name = 'main_sequence'
                """;
        List<Long> currentValues = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        });
        responseObserver.onNext(NextIdProto.newBuilder().setCurrentValue(currentValues.get(0)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateNextComponentInquiryId(UpdateNextIdRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateNextIdRequest(request);
        String sql = """
                UPDATE sequence_object SET current_value = ? WHERE name = 'main_sequence' AND current_value = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getNewCurrentValue(), request.getOldCurrentValue());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getRootCategoryIdByComponentId(GetRootCategoryIdByComponentIdRequest request,
            StreamObserver<GetRootCategoryIdByComponentIdResponse> responseObserver) {
        validateGetRootCategoryIdByComponentIdRequest(request);
        String sql = """
                select root_category_id
                from comp_catalog cc, categories pcat
                where cc.component_id = ? and cc.status_id = 102 and pcat.category_id = cc.root_category_id
                """;
        List<String> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getString(1);
        }, request.getComponentId());
        responseObserver.onNext(
                result.isEmpty() || result.get(0) == null ? GetRootCategoryIdByComponentIdResponse.getDefaultInstance()
                        : GetRootCategoryIdByComponentIdResponse.newBuilder().setRootCategoryId(result.get(0)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDefaultScorecards(Empty request, StreamObserver<GetDefaultScorecardsResponse> responseObserver) {
        String sql = """
                select ds.project_category_id, ds.scorecard_type_id, ds.scorecard_id, st.name
                from default_scorecard ds, scorecard_type_lu st
                where ds.scorecard_type_id = st.scorecard_type_id
                """;
        List<DefaultScorecardProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            DefaultScorecardProto.Builder builder = DefaultScorecardProto.newBuilder();
            ResultSetHelper.applyResultSetInt(rs, 1, builder::setProjectCategoryId);
            ResultSetHelper.applyResultSetInt(rs, 2, builder::setScorecardTypeId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setScorecardId);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setName);
            return builder.build();
        });
        responseObserver.onNext(GetDefaultScorecardsResponse.newBuilder().addAllDefaultScorecards(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteProjectResult(DeleteProjectResultRequest request, StreamObserver<CountProto> responseObserver) {
        validateDeleteProjectResultRequest(request);
        String sql = """
                delete from project_result where project_id = ? and user_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteComponentInquiry(DeleteComponentInquiryRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeleteComponentInquiryRequest(request);
        String sql = """
                delete from component_inquiry where project_id = ? and user_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProjectResultForAdvanceScreening(UpdateProjectResultForAdvanceScreeningRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateUpdateProjectResultForAdvanceScreeningRequest(request);
        String sql = """
                update project_result set rating_ind=1, valid_submission_ind=1
                where project_id=? and user_id=?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getVersionUsingComponentVersionId(GetVersionUsingComponentVersionIdRequest request,
            StreamObserver<GetVersionUsingComponentVersionIdResponse> responseObserver) {
        validateGetVersionUsingComponentVersionIdRequest(request);
        String sql = """
                select version from comp_versions where comp_vers_id = ?
                """;
        List<Integer> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getInt(1);
        }, request.getCompVersId());
        responseObserver.onNext(result.isEmpty() ? GetVersionUsingComponentVersionIdResponse.getDefaultInstance()
                : GetVersionUsingComponentVersionIdResponse.newBuilder().setVersion(result.get(0)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDeliverableIdToNameMap(Empty request,
            StreamObserver<GetDeliverableIdToNameMapResponse> responseObserver) {
        String sql = """
                SELECT deliverable_id, name FROM deliverable_lu
                """;
        List<DeliverabIdNameProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            DeliverabIdNameProto.Builder builder = DeliverabIdNameProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setDeliverableId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            return builder.build();
        });
        responseObserver.onNext(GetDeliverableIdToNameMapResponse.newBuilder().addAllDeliverables(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void logDownloadAttempt(LogDownloadAttemptRequest request, StreamObserver<CountProto> responseObserver) {
        validateLogDownloadAttemptRequest(request);
        String sql = """
                INSERT INTO project_download_audit (upload_id, user_id, ip_address, successful, date)
                VALUES (?,?,?,?,current)
                """;
        Long userId = Helper.extract(request::hasUserId, request::getUserId);
        int affected = dbAccessor.executeUpdate(sql, request.getUploadId(), userId, request.getIpAddress(),
                request.getSuccessful());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    private void validateGetUserPreferenceValueRequest(GetUserPreferenceValueRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasPreferenceId, "preference_id");
    }

    private void validateIsProjectResultExistsRequest(IsProjectResultExistsRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateIsComponentInquiryExistsRequest(IsComponentInquiryExistsRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateGetRatingsRequest(GetRatingsRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateCreateProjectResultRequest(CreateProjectResultRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasRatingInd, "rating_ind");
        Helper.assertObjectNotNull(request::hasValidSubmissionInd, "valid_submission_ind");
    }

    private void validateCreateComponentInquiryRequest(CreateComponentInquiryRequest request) {
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

    private void validateGetRootCategoryIdByComponentIdRequest(GetRootCategoryIdByComponentIdRequest request) {
        Helper.assertObjectNotNull(request::hasComponentId, "component_id");
    }

    private void validateDeleteProjectResultRequest(DeleteProjectResultRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateDeleteComponentInquiryRequest(DeleteComponentInquiryRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateUpdateProjectResultForAdvanceScreeningRequest(
            UpdateProjectResultForAdvanceScreeningRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetVersionUsingComponentVersionIdRequest(GetVersionUsingComponentVersionIdRequest request) {
        Helper.assertObjectNotNull(request::hasCompVersId, "comp_vers_id");
    }

    private void validateLogDownloadAttemptRequest(LogDownloadAttemptRequest request) {
        Helper.assertObjectNotNull(request::hasUploadId, "upload_id");
        Helper.assertObjectNotNull(request::hasIpAddress, "ip_address");
        Helper.assertObjectNotNull(request::hasSuccessful, "successful");
    }
}
