package com.topcoder.or.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.util.SerializationUtils;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.component.id.DBHelper;
import com.topcoder.onlinereview.component.id.IDGenerator;
import com.topcoder.onlinereview.component.search.SearchBundle;
import com.topcoder.onlinereview.component.search.SearchBundleManager;
import com.topcoder.onlinereview.component.search.filter.Filter;
import com.topcoder.onlinereview.grpc.review.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;
import com.topcoder.or.util.SearchBundleHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ReviewService extends ReviewServiceGrpc.ReviewServiceImplBase {
    private final DBAccessor dbAccessor;
    private final DBHelper dbHelper;
    private final SearchBundleManager searchBundleManager;

    private static final String SEARCH_BUNDLE_NAME = "Review Search Bundle";

    private SearchBundle searchBundle;
    private IDGenerator reviewIDGenerator;
    private IDGenerator reviewCommentIDGenerator;
    private IDGenerator reviewItemIDGenerator;
    private IDGenerator reviewItemCommentIDGenerator;

    public static final String REVIEW_ID_SEQ = "review_id_seq";
    public static final String REVIEW_COMMENT_ID_SEQ = "review_comment_id_seq";
    public static final String REVIEW_ITEM_ID_SEQ = "review_item_id_seq";
    public static final String REVIEW_ITEM_COMMENT_ID_SEQ = "review_item_comment_id_seq";

    public ReviewService(DBAccessor dbAccessor, DBHelper dbHelper, SearchBundleManager searchBundleManager) {
        this.dbAccessor = dbAccessor;
        this.dbHelper = dbHelper;
        this.searchBundleManager = searchBundleManager;
    }

    @PostConstruct
    public void postRun() {
        searchBundle = searchBundleManager.getSearchBundle(SEARCH_BUNDLE_NAME);
        reviewIDGenerator = new IDGenerator(REVIEW_ID_SEQ, dbHelper);
        reviewCommentIDGenerator = new IDGenerator(REVIEW_COMMENT_ID_SEQ, dbHelper);
        reviewItemIDGenerator = new IDGenerator(REVIEW_ITEM_ID_SEQ, dbHelper);
        reviewItemCommentIDGenerator = new IDGenerator(REVIEW_ITEM_COMMENT_ID_SEQ, dbHelper);
        SearchBundleHelper.setSearchableFields(searchBundle, SearchBundleHelper.REVIEW_SEARCH_BUNDLE);
    }

    @Override
    public void isReviewExists(ReviewIdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateReviewIdProto(request);
        boolean result = checkEntityExists("review", "review_id", request.getReviewId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isReviewItemExists(ReviewItemIdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateReviewItemIdProto(request);
        boolean result = checkEntityExists("review_item", "review_item_id", request.getReviewItemId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void countReviewComments(ReviewIdProto request, StreamObserver<CountProto> responseObserver) {
        validateReviewIdProto(request);
        int result = getCount("review_comment", "review_id", request.getReviewId());
        responseObserver.onNext(CountProto.newBuilder().setCount(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void countReviewItemComments(ReviewItemIdProto request, StreamObserver<CountProto> responseObserver) {
        validateReviewItemIdProto(request);
        int result = getCount("review_item_comment", "review_item_id", request.getReviewItemId());
        responseObserver.onNext(CountProto.newBuilder().setCount(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviews(ReviewIdsProto request, StreamObserver<GetReviewsResponse> responseObserver) {
        validateReviewIdsProto(request);
        String sql = """
                SELECT review_id, resource_id, submission_id, project_phase_id, scorecard_id, committed, score, initial_score,
                create_user, create_date, modify_user, modify_date
                FROM review
                WHERE review_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getReviewIdsCount());
        List<ReviewProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> loadReview(rs),
                request.getReviewIdsList().toArray());
        responseObserver.onNext(GetReviewsResponse.newBuilder().addAllReviews(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewComments(ReviewIdsProto request, StreamObserver<GetReviewCommentsResponse> responseObserver) {
        validateReviewIdsProto(request);
        String sql = """
                SELECT review_comment_id, resource_id, review_id, comment_type_id, content, extra_info
                FROM review_comment
                WHERE review_id IN (%s)
                ORDER BY review_id, sort
                """;
        String inSql = Helper.getInClause(request.getReviewIdsCount());
        List<ReviewCommentProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ReviewCommentProto.Builder builder = ReviewCommentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewCommentId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setReviewId);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setCommentTypeId);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setContent);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setExtraInfo);
            return builder.build();
        }, request.getReviewIdsList().toArray());
        responseObserver.onNext(GetReviewCommentsResponse.newBuilder().addAllReviewComments(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewCommentsWithNoContent(ReviewIdsProto request,
            StreamObserver<GetReviewCommentsResponse> responseObserver) {
        validateReviewIdsProto(request);
        String sql = """
                SELECT review_comment_id, resource_id, review_id, comment_type_id
                FROM review_comment
                WHERE review_id IN (%s)
                ORDER BY review_id, sort
                """;
        String inSql = Helper.getInClause(request.getReviewIdsCount());
        List<ReviewCommentProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ReviewCommentProto.Builder builder = ReviewCommentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewCommentId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setReviewId);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setCommentTypeId);
            return builder.build();
        }, request.getReviewIdsList().toArray());
        responseObserver.onNext(GetReviewCommentsResponse.newBuilder().addAllReviewComments(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewItems(ReviewIdsProto request, StreamObserver<GetReviewItemsResponse> responseObserver) {
        validateReviewIdsProto(request);
        String sql = """
                SELECT review_item_id, review_id, scorecard_question_id, upload_id, answer
                FROM review_item
                WHERE review_id IN (%s)
                ORDER BY review_id, sort
                """;
        String inSql = Helper.getInClause(request.getReviewIdsCount());
        List<ReviewItemProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ReviewItemProto.Builder builder = ReviewItemProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewItemId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setReviewId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setScorecardQuestionId);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setUploadId);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setAnswer);
            return builder.build();
        }, request.getReviewIdsList().toArray());
        responseObserver.onNext(GetReviewItemsResponse.newBuilder().addAllReviewItems(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewItemComments(ReviewIdsProto request,
            StreamObserver<GetReviewItemCommentsResponse> responseObserver) {
        validateReviewIdsProto(request);
        String sql = """
                SELECT ric.review_item_comment_id, ric.resource_id, ric.review_item_id, ric.comment_type_id, ric.content, ric.extra_info
                FROM review_item_comment ric
                INNER JOIN review_item ri ON ric.review_item_id=ri.review_item_id AND ri.review_id IN (%s)
                ORDER BY ric.review_item_id, ric.sort
                """;
        String inSql = Helper.getInClause(request.getReviewIdsCount());
        List<ReviewItemCommentProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ReviewItemCommentProto.Builder builder = ReviewItemCommentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewItemCommentId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setReviewItemId);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setCommentTypeId);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setContent);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setExtraInfo);
            return builder.build();
        }, request.getReviewIdsList().toArray());
        responseObserver.onNext(GetReviewItemCommentsResponse.newBuilder().addAllReviewItemComments(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewItemCommentsWithNoContent(ReviewIdsProto request,
            StreamObserver<GetReviewItemCommentsResponse> responseObserver) {
        validateReviewIdsProto(request);
        String sql = """
                SELECT ric.review_item_comment_id, ric.resource_id, ric.review_item_id, ric.comment_type_id
                FROM review_item_comment ric
                INNER JOIN review_item ri ON ric.review_item_id=ri.review_item_id AND ri.review_id IN (%s)
                ORDER BY ric.review_item_id, ric.sort
                """;
        String inSql = Helper.getInClause(request.getReviewIdsCount());
        List<ReviewItemCommentProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ReviewItemCommentProto.Builder builder = ReviewItemCommentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setReviewItemCommentId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setReviewItemId);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setCommentTypeId);
            return builder.build();
        }, request.getReviewIdsList().toArray());
        responseObserver.onNext(GetReviewItemCommentsResponse.newBuilder().addAllReviewItemComments(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllCommentTypes(Empty request, StreamObserver<GetAllCommentTypesResponse> responseObserver) {
        String sql = """
                SELECT comment_type_id, name FROM comment_type_lu
                """;
        List<CommentTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            CommentTypeProto.Builder builder = CommentTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setCommentTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            return builder.build();
        });
        responseObserver.onNext(GetAllCommentTypesResponse.newBuilder().addAllCommentTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewCommentIds(ReviewIdProto request, StreamObserver<IdListProto> responseObserver) {
        validateReviewIdProto(request);
        String sql = """
                SELECT review_comment_id FROM review_comment WHERE review_id = ?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getReviewId());
        responseObserver.onNext(IdListProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewItemIds(ReviewIdProto request, StreamObserver<IdListProto> responseObserver) {
        validateReviewIdProto(request);
        List<Long> result = getReviewItemIds(request.getReviewId());
        responseObserver.onNext(IdListProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewItemCommentIds(ReviewItemIdProto request, StreamObserver<IdListProto> responseObserver) {
        validateReviewItemIdProto(request);
        String sql = """
                SELECT review_item_comment_id FROM review_item_comment WHERE review_item_id = ?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getReviewItemId());
        responseObserver.onNext(IdListProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getReviewItemsUploadIds(ReviewItemIdsProto request, StreamObserver<IdListProto> responseObserver) {
        validateReviewItemIdsProto(request);
        List<Long> result = getReviewItemsUploadIds(request.getReviewItemIdsList());
        responseObserver.onNext(IdListProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createReview(ReviewProto request, StreamObserver<IdProto> responseObserver) {
        validateCreateReviewRequest(request);
        long newId = reviewIDGenerator.getNextID();
        String sql = """
                INSERT INTO review (review_id, resource_id, submission_id, project_phase_id, scorecard_id, committed, score, initial_score,
                create_user, create_date, modify_user, modify_date)
                values (?,?,?,?,?,?,?,?,?,?,?,?)
                """;
        Long submissionId = Helper.extract(request::hasSubmissionId, request::getSubmissionId);
        Double score = Helper.extract(request::hasScore, request::getScore);
        Double initialScore = Helper.extract(request::hasInitialScore, request::getInitialScore);
        dbAccessor.executeUpdate(sql, newId, request.getResourceId(), submissionId, request.getProjectPhaseId(),
                request.getScorecardId(), request.getCommitted() ? 1 : 0, score, initialScore, request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(IdProto.newBuilder().setId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createReviewComment(ReviewCommentProto request, StreamObserver<IdProto> responseObserver) {
        validateCreateReviewCommentRequest(request);
        long newId = reviewCommentIDGenerator.getNextID();
        String sql = """
                INSERT INTO review_comment (review_comment_id, resource_id, review_id, comment_type_id, content, extra_info, sort,
                create_user, create_date, modify_user, modify_date)
                values (?,?,?,?,?,?,?,?,?,?,?)
                """;
        String extraInfo = Helper.extract(request::hasExtraInfo, request::getExtraInfo);
        dbAccessor.executeUpdate(sql, newId, request.getResourceId(), request.getReviewId(), request.getCommentTypeId(),
                request.getContent(), extraInfo, request.getSort(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(IdProto.newBuilder().setId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createReviewItem(ReviewItemProto request, StreamObserver<IdProto> responseObserver) {
        validateCreateReviewItemRequest(request);
        long newId = reviewItemIDGenerator.getNextID();
        String sql = """
                INSERT INTO review_item (review_item_id, review_id, scorecard_question_id, upload_id, answer, sort,
                create_user, create_date, modify_user, modify_date)
                values (?,?,?,?,?,?,?,?,?,?)
                """;
        Long uploadId = Helper.extract(request::hasUploadId, request::getUploadId);
        dbAccessor.executeUpdate(sql, newId, request.getReviewId(), request.getScorecardQuestionId(), uploadId,
                request.getAnswer(), request.getSort(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(IdProto.newBuilder().setId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createReviewItemComment(ReviewItemCommentProto request, StreamObserver<IdProto> responseObserver) {
        validateCreateReviewItemCommentRequest(request);
        long newId = reviewItemCommentIDGenerator.getNextID();
        String sql = """
                INSERT INTO review_item_comment (review_item_comment_id, resource_id, review_item_id, comment_type_id, content, extra_info, sort,
                create_user, create_date, modify_user, modify_date)
                values (?,?,?,?,?,?,?,?,?,?,?)
                """;
        String extraInfo = Helper.extract(request::hasExtraInfo, request::getExtraInfo);
        dbAccessor.executeUpdate(sql, newId, request.getResourceId(), request.getReviewItemId(),
                request.getCommentTypeId(), request.getContent(), extraInfo, request.getSort(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(IdProto.newBuilder().setId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateReview(ReviewProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateReviewRequest(request);
        String sql = """
                UPDATE review SET resource_id=?, submission_id=?, project_phase_id = ?, scorecard_id=?, committed=?, score=?, initial_score=?,
                modify_user=?, modify_date=?
                WHERE review_id=?
                """;
        Long submissionId = Helper.extract(request::hasSubmissionId, request::getSubmissionId);
        Double score = Helper.extract(request::hasScore, request::getScore);
        Double initialScore = Helper.extract(request::hasInitialScore, request::getInitialScore);
        int affected = dbAccessor.executeUpdate(sql, request.getResourceId(), submissionId, request.getProjectPhaseId(),
                request.getScorecardId(), request.getCommitted() ? 1 : 0, score, initialScore, request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getReviewId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateReviewComment(ReviewCommentProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateReviewCommentRequest(request);
        String sql = """
                UPDATE review_comment SET resource_id=?, comment_type_id=?, content=?, extra_info=?, sort=?, modify_user=?, modify_date=?
                WHERE review_comment_id=?
                    """;
        String extraInfo = Helper.extract(request::hasExtraInfo, request::getExtraInfo);
        int affected = dbAccessor.executeUpdate(sql, request.getResourceId(), request.getCommentTypeId(),
                request.getContent(),
                extraInfo, request.getSort(), request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getReviewCommentId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateReviewItem(ReviewItemProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateReviewItemRequest(request);
        String sql = """
                UPDATE review_item SET scorecard_question_id=?, upload_id=?, answer=?, sort=?, modify_user=?, modify_date=?
                WHERE review_item_id=?
                """;
        Long uploadId = Helper.extract(request::hasUploadId, request::getUploadId);
        int affected = dbAccessor.executeUpdate(sql, request.getScorecardQuestionId(), uploadId, request.getAnswer(),
                request.getSort(), request.getModifyUser(), Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateReviewItemComment(ReviewItemCommentProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateReviewItemCommentRequest(request);
        String sql = """
                UPDATE review_item_comment SET resource_id=?, comment_type_id=?, content=?, extra_info=?, sort=?, modify_user=?, modify_date=?
                WHERE review_item_comment_id=?
                """;
        String extraInfo = Helper.extract(request::hasExtraInfo, request::getExtraInfo);
        int affected = dbAccessor.executeUpdate(sql, request.getResourceId(), request.getCommentTypeId(),
                request.getContent(), extraInfo, request.getSort(), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteReview(ReviewIdProto request, StreamObserver<CountProto> responseObserver) {
        validateReviewIdProto(request);
        List<Long> reviewItemIds = getReviewItemIds(request.getReviewId());
        deleteReviewItems(reviewItemIds);
        deleteReviewComments(request.getReviewId());
        String sql = """
                DELETE FROM review WHERE review_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getReviewId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteReviewComments(ReviewCommentIdsProto request, StreamObserver<CountProto> responseObserver) {
        validateReviewCommentIdsProto(request);
        String sql = """
                DELETE FROM review_comment WHERE review_comment_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getReviewCommentIdsCount());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), request.getReviewCommentIdsList().toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteReviewItems(ReviewItemIdsProto request, StreamObserver<CountProto> responseObserver) {
        validateReviewItemIdsProto(request);
        int affected = deleteReviewItems(request.getReviewItemIdsList());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteReviewItemComments(ReviewItemCommentIdsProto request,
            StreamObserver<CountProto> responseObserver) {
        validateReviewItemCommentIdsProto(request);
        String sql = """
                DELETE FROM review_item_comment WHERE review_item_comment_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getReviewItemCommentIdsCount());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), request.getReviewItemCommentIdsList().toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchReviews(FilterProto request, StreamObserver<GetReviewsResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<ReviewProto> result = searchBundle.search(filter, (rs, _i) -> loadReview(rs));
        responseObserver.onNext(GetReviewsResponse.newBuilder().addAllReviews(result).build());
        responseObserver.onCompleted();
    }

    private ReviewProto loadReview(ResultSet rs) throws SQLException {
        ReviewProto.Builder builder = ReviewProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "review_id", builder::setReviewId);
        ResultSetHelper.applyResultSetLong(rs, "resource_id", builder::setResourceId);
        ResultSetHelper.applyResultSetLong(rs, "submission_id", builder::setSubmissionId);
        ResultSetHelper.applyResultSetLong(rs, "project_phase_id", builder::setProjectPhaseId);
        ResultSetHelper.applyResultSetLong(rs, "scorecard_id", builder::setScorecardId);
        ResultSetHelper.applyResultSetBool(rs, "committed", builder::setCommitted);
        ResultSetHelper.applyResultSetDouble(rs, "score", builder::setScore);
        ResultSetHelper.applyResultSetDouble(rs, "initial_score", builder::setInitialScore);
        ResultSetHelper.applyResultSetString(rs, "create_user", builder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "modify_user", builder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "modify_date", builder::setModifyDate);
        return builder.build();
    }

    private List<Long> getReviewItemIds(long reviewId) {
        String sql = """
                SELECT review_item_id FROM review_item WHERE review_id = ?
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, reviewId);

    }

    private List<Long> getReviewItemsUploadIds(List<Long> reviewItemIds) {
        String sql = """
                SELECT upload_id FROM review_item  WHERE review_item_id IN (%s)
                """;
        String inSql = Helper.getInClause(reviewItemIds.size());
        return dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            long id = rs.getLong(1);
            if (rs.wasNull()) {
                return null;
            } else {
                return id;
            }
        }, reviewItemIds.toArray());
    }

    private int deleteReviewComments(long reviewId) {
        String sql = """
                DELETE FROM review_comment WHERE review_id = ?
                """;
        return dbAccessor.executeUpdate(sql, reviewId);
    }

    private int deleteReviewItems(List<Long> reviewItemIds) {
        if (reviewItemIds.isEmpty()) {
            return 0;
        }
        deleteReviewItemComments(reviewItemIds);
        List<Long> uploadIds = getReviewItemsUploadIds(reviewItemIds);
        deleteReviewItemUploads(uploadIds);
        String sql = """
                DELETE FROM review_item WHERE review_item_id IN (%s)
                """;
        String inSql = Helper.getInClause(reviewItemIds.size());
        return dbAccessor.executeUpdate(sql.formatted(inSql), reviewItemIds.toArray());
    }

    private int deleteReviewItemComments(List<Long> reviewItemIds) {
        if (reviewItemIds.isEmpty()) {
            return 0;
        }
        String sql = """
                DELETE FROM review_item_comment WHERE review_item_id IN (%s)
                """;
        String inSql = Helper.getInClause(reviewItemIds.size());
        return dbAccessor.executeUpdate(sql.formatted(inSql), reviewItemIds.toArray());
    }

    private int deleteReviewItemUploads(List<Long> reviewItemUploadIds) {
        if (reviewItemUploadIds.isEmpty()) {
            return 0;
        }
        String sql = """
                DELETE FROM upload WHERE upload_id IN (%s)
                """;
        String inSql = Helper.getInClause(reviewItemUploadIds.size());
        return dbAccessor.executeUpdate(sql.formatted(inSql), reviewItemUploadIds.toArray());
    }

    private boolean checkEntityExists(String tableName, String columnName, long id) {
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM %s WHERE %s = ?) THEN 1 ELSE 0 END FROM DUAL
                """.formatted(tableName, columnName);
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, id).get(0);
    }

    private int getCount(String tableName, String columnName, long id) {
        String sql = """
                SELECT COUNT(1) FROM %s WHERE %s = ?
                """.formatted(tableName, columnName);
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getInt(1);
        }, id).get(0);
    }

    private void validateReviewIdProto(ReviewIdProto request) {
        Helper.assertObjectNotNull(request::hasReviewId, "review_id");
    }

    private void validateReviewItemIdProto(ReviewItemIdProto request) {
        Helper.assertObjectNotNull(request::hasReviewItemId, "review_item_id");
    }

    private void validateReviewIdsProto(ReviewIdsProto request) {
        Helper.assertObjectNotEmpty(request::getReviewIdsCount, "review_ids");
    }

    private void validateReviewItemIdsProto(ReviewItemIdsProto request) {
        Helper.assertObjectNotEmpty(request::getReviewItemIdsCount, "review_item_ids");
    }

    private void validateReviewCommentIdsProto(ReviewCommentIdsProto request) {
        Helper.assertObjectNotEmpty(request::getReviewCommentIdsCount, "review_comment_ids");
    }

    private void validateReviewItemCommentIdsProto(ReviewItemCommentIdsProto request) {
        Helper.assertObjectNotEmpty(request::getReviewItemCommentIdsCount, "review_item_comment_ids");
    }

    private void validateCreateReviewRequest(ReviewProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasResourceId, request::getResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasResourceId, "project_phase_id");
        Helper.assertObjectNotNullAndPositive(request::hasScorecardId, request::getScorecardId, "scorecard_id");
        Helper.assertObjectNotNull(request::hasCommitted, "committed");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateReviewCommentRequest(ReviewCommentProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasResourceId, request::getResourceId, "resource_id");
        Helper.assertObjectNotNullAndPositive(request::hasReviewId, request::getReviewId, "review_id");
        Helper.assertObjectNotNullAndPositive(request::hasCommentTypeId, request::getCommentTypeId, "comment_type_id");
        Helper.assertObjectNotNull(request::hasContent, "content");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateReviewItemRequest(ReviewItemProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasReviewId, request::getReviewId, "review_id");
        Helper.assertObjectNotNullAndPositive(request::hasScorecardQuestionId, request::getScorecardQuestionId,
                "scorecard_question_id");
        Helper.assertObjectNotNull(request::hasAnswer, "answer");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateReviewItemCommentRequest(ReviewItemCommentProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasResourceId, request::getResourceId, "resource_id");
        Helper.assertObjectNotNullAndPositive(request::hasReviewItemId, request::getReviewItemId, "review_item_id");
        Helper.assertObjectNotNullAndPositive(request::hasCommentTypeId, request::getCommentTypeId, "comment_type_id");
        Helper.assertObjectNotNull(request::hasContent, "content");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateReviewRequest(ReviewProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasReviewId, request::getReviewId, "review_id");
        Helper.assertObjectNotNullAndPositive(request::hasResourceId, request::getResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasResourceId, "project_phase_id");
        Helper.assertObjectNotNullAndPositive(request::hasScorecardId, request::getScorecardId, "scorecard_id");
        Helper.assertObjectNotNull(request::hasCommitted, "committed");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateReviewCommentRequest(ReviewCommentProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasReviewCommentId, request::getReviewCommentId,
                "review_comment_id");
        Helper.assertObjectNotNullAndPositive(request::hasResourceId, request::getResourceId, "resource_id");
        Helper.assertObjectNotNullAndPositive(request::hasCommentTypeId, request::getCommentTypeId, "comment_type_id");
        Helper.assertObjectNotNull(request::hasContent, "content");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateReviewItemRequest(ReviewItemProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasReviewItemId, request::getReviewItemId, "review_item_id");
        Helper.assertObjectNotNullAndPositive(request::hasScorecardQuestionId, request::getScorecardQuestionId,
                "scorecard_question_id");
        Helper.assertObjectNotNull(request::hasAnswer, "answer");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateReviewItemCommentRequest(ReviewItemCommentProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasReviewItemCommentId, request::getReviewItemCommentId,
                "review_item_comment_id");
        Helper.assertObjectNotNullAndPositive(request::hasResourceId, request::getResourceId, "resource_id");
        Helper.assertObjectNotNullAndPositive(request::hasCommentTypeId, request::getCommentTypeId, "comment_type_id");
        Helper.assertObjectNotNull(request::hasContent, "content");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }
}
