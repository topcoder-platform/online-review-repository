package com.topcoder.or.repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.topcoder.onlinereview.grpc.reviewfeedback.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ReviewFeedbackService extends ReviewFeedbackServiceGrpc.ReviewFeedbackServiceImplBase {
    private final DBAccessor dbAccessor;

    public ReviewFeedbackService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void getReviewFeedback(ReviewFeedbackIdProto request, StreamObserver<ReviewFeedbackProto> responseObserver) {
        validateReviewFeedbackIdProto(request);
        String sql = """
                SELECT review_feedback_id, project_id, comment, create_user, create_date, modify_user, modify_date
                FROM review_feedback
                WHERE review_feedback_id = ?
                """;
        List<ReviewFeedbackProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ReviewFeedbackProto.Builder builder = ReviewFeedbackProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewFeedbackId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setProjectId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setComment);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 5, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 7, builder::setModifyDate);
            return builder.build();
        }, request.getReviewFeedbackId());
        responseObserver.onNext(result.isEmpty() ? ReviewFeedbackProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewFeedbackByProjectId(ProjectIdProto request,
            StreamObserver<ReviewFeedbacksProto> responseObserver) {
        validateProjectIdProto(request);
        String sql = """
                SELECT review_feedback_id, project_id, comment, create_user, create_date, modify_user, modify_date
                FROM review_feedback
                WHERE project_id = ?
                """;
        List<ReviewFeedbackProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ReviewFeedbackProto.Builder builder = ReviewFeedbackProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewFeedbackId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setProjectId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setComment);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 5, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 7, builder::setModifyDate);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(ReviewFeedbacksProto.newBuilder().addAllReviewFeedbacks(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewFeedbackDetails(ReviewFeedbackIdProto request,
            StreamObserver<ReviewFeedbackDetailsProto> responseObserver) {
        validateReviewFeedbackIdProto(request);
        String sql = """
                SELECT review_feedback_id, reviewer_user_id, score, feedback_text
                FROM review_feedback_detail
                WHERE review_feedback_id = ?
                """;
        List<ReviewFeedbackDetailProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ReviewFeedbackDetailProto.Builder builder = ReviewFeedbackDetailProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewFeedbackId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setReviewerUserId);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setScore);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setFeedbackText);
            return builder.build();
        }, request.getReviewFeedbackId());
        responseObserver.onNext(ReviewFeedbackDetailsProto.newBuilder().addAllReviewFeedbackDetails(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewFeedbackDetailsByProjectId(ProjectIdProto request,
            StreamObserver<ReviewFeedbackDetailsProto> responseObserver) {
        validateProjectIdProto(request);
        String sql = """
                SELECT rfd.review_feedback_id, rfd.reviewer_user_id, rfd.score, rfd.feedback_text
                FROM review_feedback rf
                INNER JOIN review_feedback_detail rfd ON rf.review_feedback_id = rfd.review_feedback_id
                WHERE rf.project_id  = ?
                """;
        List<ReviewFeedbackDetailProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ReviewFeedbackDetailProto.Builder builder = ReviewFeedbackDetailProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewFeedbackId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setReviewerUserId);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setScore);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setFeedbackText);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(ReviewFeedbackDetailsProto.newBuilder().addAllReviewFeedbackDetails(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewerIdsByFeedbackId(ReviewFeedbackIdProto request,
            StreamObserver<ReviewerIdsProto> responseObserver) {
        validateReviewFeedbackIdProto(request);
        String sql = """
                SELECT reviewer_user_id
                FROM review_feedback_detail
                WHERE review_feedback_id = ?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getReviewFeedbackId());
        responseObserver.onNext(ReviewerIdsProto.newBuilder().addAllReviewerUserIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createReviewFeedback(ReviewFeedbackProto request,
            StreamObserver<ReviewFeedbackIdProto> responseObserver) {
        validateCreateReviewFeedbackProto(request);
        String sql = """
                INSERT INTO review_feedback (project_id, comment, create_user, create_date, modify_user, modify_date)
                VALUES (?,?,?,?,?,?)
                """;
        String comment = Helper.extract(request::hasComment, request::getComment);
        Number id = dbAccessor.executeUpdateReturningKey(sql, conn -> {
            PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, request.getProjectId());
            ps.setString(2, comment);
            ps.setString(3, request.getCreateUser());
            ps.setTimestamp(4, Helper.convertTimestamp(request.getCreateDate()));
            ps.setString(5, request.getModifyUser());
            ps.setTimestamp(6, Helper.convertTimestamp(request.getModifyDate()));
            return ps;
        });
        responseObserver.onNext(ReviewFeedbackIdProto.newBuilder().setReviewFeedbackId(id.longValue()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createReviewFeedbackDetail(ReviewFeedbackDetailProto request,
            StreamObserver<CountProto> responseObserver) {
        validateReviewFeedbackDetailProto(request);
        String sql = """
                INSERT INTO review_feedback_detail (review_feedback_id, reviewer_user_id, score, feedback_text)
                VALUES (?,?,?,?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getReviewFeedbackId(),
                request.getReviewerUserId(), request.getScore(), request.getFeedbackText());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void auditReviewFeedback(ReviewFeedbackAuditProto request, StreamObserver<CountProto> responseObserver) {
        validateReviewFeedbackAuditProto(request);
        String sql = """
                INSERT INTO review_feedback_audit (review_feedback_id, project_id, comment, audit_action_type_id, action_user, action_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getReviewFeedbackId(), request.getProjectId(),
                request.getComment(), request.getAuditActionTypeId(), request.getActionUser(),
                Helper.convertDate(request.getActionDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void auditReviewFeedbackDetail(ReviewFeedbackDetailAuditProto request,
            StreamObserver<CountProto> responseObserver) {
        validateReviewFeedbackDetailAuditProto(request);
        String sql = """
                INSERT INTO review_feedback_detail_audit (review_feedback_id, reviewer_user_id, score, feedback_text, audit_action_type_id, action_user, action_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        Integer score = Helper.extract(request::hasScore, request::getScore);
        String feedbackText = Helper.extract(request::hasFeedbackText, request::getFeedbackText);
        int affected = dbAccessor.executeUpdate(sql, request.getReviewFeedbackId(), request.getReviewerUserId(), score,
                feedbackText, request.getAuditActionTypeId(), request.getActionUser(),
                Helper.convertDate(request.getActionDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateReviewFeedback(ReviewFeedbackProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateReviewFeedbackProto(request);
        String sql = """
                UPDATE review_feedback
                SET project_id = ?, comment = ?, modify_user = ?, modify_date = ?
                WHERE review_feedback_id = ?
                """;
        String comment = Helper.extract(request::hasComment, request::getComment);
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), comment, request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateReviewFeedbackDetail(ReviewFeedbackDetailProto request,
            StreamObserver<CountProto> responseObserver) {
        validateReviewFeedbackDetailProto(request);
        String sql = """
                UPDATE review_feedback_detail
                SET score = ?, feedback_text = ?
                WHERE review_feedback_id = ? AND reviewer_user_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getScore(), request.getFeedbackText(),
                request.getReviewFeedbackId(), request.getReviewerUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteReviewFeedback(ReviewFeedbackIdProto request, StreamObserver<CountProto> responseObserver) {
        validateReviewFeedbackIdProto(request);
        final long reviewFeedbackId = request.getReviewFeedbackId();
        deleteReviewFeedbackAudit(reviewFeedbackId);
        deleteReviewFeedbackDetailAudit(reviewFeedbackId);
        deleteReviewFeedbackDetail(reviewFeedbackId);
        int affected = deleteReviewFeedback(reviewFeedbackId);
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteReviewFeedbackDetail(DeleteReviewFeedbackDetailRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeleteReviewFeedbackDetailRequest(request);
        String sql = """
                DELETE FROM review_feedback_detail WHERE review_feedback_id = ? AND reviewer_user_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getReviewerUserIdsCount());
        List<Object> param = new ArrayList<>();
        param.add(request.getReviewFeedbackId());
        param.addAll(request.getReviewerUserIdsList());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), param.toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    private int deleteReviewFeedbackAudit(long reviewFeedbackId) {
        String sql = """
                DELETE FROM review_feedback_audit WHERE review_feedback_id = ?
                """;
        return dbAccessor.executeUpdate(sql, reviewFeedbackId);
    }

    private int deleteReviewFeedbackDetailAudit(long reviewFeedbackId) {
        String sql = """
                DELETE FROM review_feedback_detail_audit WHERE review_feedback_id = ?
                """;
        return dbAccessor.executeUpdate(sql, reviewFeedbackId);
    }

    private int deleteReviewFeedbackDetail(long reviewFeedbackId) {
        String sql = """
                DELETE FROM review_feedback_detail WHERE review_feedback_id = ?
                """;
        return dbAccessor.executeUpdate(sql, reviewFeedbackId);
    }

    private int deleteReviewFeedback(long reviewFeedbackId) {
        String sql = """
                DELETE FROM review_feedback WHERE review_feedback_id = ?
                """;
        return dbAccessor.executeUpdate(sql, reviewFeedbackId);
    }

    private void validateReviewFeedbackIdProto(ReviewFeedbackIdProto request) {
        Helper.assertObjectNotNull(request::hasReviewFeedbackId, "review_feedback_id");
    }

    private void validateProjectIdProto(ProjectIdProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateCreateReviewFeedbackProto(ReviewFeedbackProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateReviewFeedbackDetailProto(ReviewFeedbackDetailProto request) {
        Helper.assertObjectNotNull(request::hasReviewFeedbackId, "review_feedback_id");
        Helper.assertObjectNotNull(request::hasReviewerUserId, "reviewer_user_id");
        Helper.assertObjectNotNull(request::hasScore, "score");
        Helper.assertObjectNotNull(request::hasFeedbackText, "feedback_text");
    }

    private void validateReviewFeedbackAuditProto(ReviewFeedbackAuditProto request) {
        Helper.assertObjectNotNull(request::hasReviewFeedbackId, "review_feedback_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasAuditActionTypeId, "audit_action_type_id");
        Helper.assertObjectNotNull(request::hasActionUser, "action_user");
        Helper.assertObjectNotNull(request::hasActionDate, "action_date");
    }

    private void validateReviewFeedbackDetailAuditProto(ReviewFeedbackDetailAuditProto request) {
        Helper.assertObjectNotNull(request::hasReviewFeedbackId, "review_feedback_id");
        Helper.assertObjectNotNull(request::hasReviewerUserId, "reviewer_user_id");
        Helper.assertObjectNotNull(request::hasAuditActionTypeId, "audit_action_type_id");
        Helper.assertObjectNotNull(request::hasActionUser, "action_user");
        Helper.assertObjectNotNull(request::hasActionDate, "action_date");
    }

    private void validateUpdateReviewFeedbackProto(ReviewFeedbackProto request) {
        Helper.assertObjectNotNull(request::hasReviewFeedbackId, "review_feedback_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateDeleteReviewFeedbackDetailRequest(DeleteReviewFeedbackDetailRequest request) {
        Helper.assertObjectNotNull(request::hasReviewFeedbackId, "review_feedback_id");
        Helper.assertObjectNotEmpty(request::getReviewerUserIdsCount, "reviewer_user_ids");
    }
}
