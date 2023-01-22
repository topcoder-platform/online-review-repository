package com.topcoder.or.repository;

import java.util.List;

import com.topcoder.onlinereview.grpc.phasehandler.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class PhaseHandlerService extends PhaseHandlerServiceGrpc.PhaseHandlerServiceImplBase {
    private final DBAccessor dbAccessor;

    public PhaseHandlerService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void updateProjectResultPayment(UpdateProjectResultPaymentRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateUpdateProjectResultPaymentRequest(request);
        String sql = """
                UPDATE project_result SET payment = ?
                WHERE project_id = ? AND user_id = ?
                """;
        Double payment = Helper.extract(request::hasPayment, request::getPayment);
        int affected = dbAccessor.executeUpdate(sql, payment, request.getProjectId(), request.getUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProjectResult(UpdateProjectResultRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateProjectResultRequest(request);
        String sql = """
                update project_result set valid_submission_ind = 0
                where not exists(select * from submission s,upload u,resource r,resource_info ri where u.upload_id = s.upload_id
                and upload_type_id = 1 and s.submission_type_id = 1 and u.project_id = project_result.project_id
                and r.resource_id = u.resource_id and ri.resource_id = r.resource_id  and ri.value = project_result.user_id
                and ri.resource_info_type_id = 1 and submission_status_id <> 5 )
                and project_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateFailedPassScreening(UpdateFailedPassScreeningRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateUpdateFailedPassScreeningRequest(request);
        String sql = """
                update project_result set valid_submission_ind = 0, rating_ind = 0
                where exists(select * from submission s,upload u,resource r,resource_info ri
                where u.upload_id = s.upload_id and u.upload_type_id = 1 and s.submission_type_id = 1
                and u.project_id = project_result.project_id and r.resource_id = u.resource_id and ri.resource_id = r.resource_id
                and ri.value = project_result.user_id and ri.resource_info_type_id = 1 and submission_status_id = 2 )
                and project_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePassScreening(UpdatePassScreeningRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdatePassScreeningRequest(request);
        String sql = """
                update project_result set valid_submission_ind = 1, rating_ind = 1
                where exists(select * from submission s,upload u,resource r,resource_info ri
                where u.upload_id = s.upload_id and u.upload_type_id = 1 and s.submission_type_id = 1
                and u.project_id = project_result.project_id and r.resource_id = u.resource_id
                and ri.resource_id = r.resource_id  and ri.value = project_result.user_id
                and ri.resource_info_type_id = 1 and submission_status_id in (1,3,4) )
                and project_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviews(GetReviewsRequest request, StreamObserver<GetReviewsResponse> responseObserver) {
        validateGetReviewsRequest(request);
        String sql = """
                select s.initial_score as raw_score, ri_u.value as user_id, r.project_id
                from resource r, resource_info ri_u, upload u, submission s
                where r.resource_id = ri_u.resource_id and ri_u.resource_info_type_id = 1
                and s.submission_type_id = 1 and u.project_id = r.project_id and u.resource_id = r.resource_id
                and upload_type_id = 1 and u.upload_id = s.upload_id  and r.project_id = ?
                """;
        List<ReviewProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ReviewProto.Builder builder = ReviewProto.newBuilder();
            ResultSetHelper.applyResultSetDouble(rs, 1, builder::setRawScore);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setUserId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setProjectId);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(GetReviewsResponse.newBuilder().addAllReviews(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProjectResultReview(UpdateProjectResultReviewRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateUpdateProjectResultReviewRequest(request);
        String sql = """
                update project_result set valid_submission_ind = 1, rating_ind = 1, raw_score = ?
                where project_id = ? and user_id = ?
                """;
        Double rawScore = Helper.extract(request::hasRawScore, request::getRawScore);
        int affected = dbAccessor.executeUpdate(sql, rawScore, request.getProjectId(), request.getUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAppealResponses(GetAppealResponsesRequest request,
            StreamObserver<GetAppealResponsesResponse> responseObserver) {
        validateGetAppealResponsesRequest(request);
        String sql = """
                select s.final_score as final_score, ri_u.value as user_id, s.placement as placed, r.project_id, s.submission_status_id
                from resource r, resource_info ri_u, upload u, submission s
                where r.resource_id = ri_u.resource_id and s.submission_type_id = 1 and ri_u.resource_info_type_id = 1
                and u.project_id = r.project_id and u.resource_id = r.resource_id and upload_type_id = 1
                and u.upload_id = s.upload_id and r.project_id = ? and r.resource_role_id = 1 and s.submission_status_id != 5
                """;
        List<AppealResponseProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            AppealResponseProto.Builder builder = AppealResponseProto.newBuilder();
            ResultSetHelper.applyResultSetDouble(rs, 1, builder::setFinalScore);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setUserId);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setPlacement);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setProjectId);
            ResultSetHelper.applyResultSetInt(rs, 5, builder::setSubmissionStatusId);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(GetAppealResponsesResponse.newBuilder().addAllAppealResponses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProjectResultAppealResponse(UpdateProjectResultAppealResponseRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateUpdateProjectResultAppealResponseRequest(request);
        String sql = """
                update project_result set final_score = ?, placed = ?, passed_review_ind = ?
                where project_id = ? and user_id = ?
                """;
        Double finalScore = Helper.extract(request::hasFinalScore, request::getFinalScore);
        Integer placed = Helper.extract(request::hasPlaced, request::getPlaced);
        Integer passedReviewInd = Helper.extract(request::hasPassedReviewInd, request::getPassedReviewInd);
        int affected = dbAccessor.executeUpdate(sql, finalScore, placed, passedReviewInd, request.getProjectId(),
                request.getUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    private void validateUpdateProjectResultPaymentRequest(UpdateProjectResultPaymentRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateUpdateProjectResultRequest(UpdateProjectResultRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateUpdateFailedPassScreeningRequest(UpdateFailedPassScreeningRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateUpdatePassScreeningRequest(UpdatePassScreeningRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateGetReviewsRequest(GetReviewsRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateUpdateProjectResultReviewRequest(UpdateProjectResultReviewRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetAppealResponsesRequest(GetAppealResponsesRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateUpdateProjectResultAppealResponseRequest(UpdateProjectResultAppealResponseRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }
}
