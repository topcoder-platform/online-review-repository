package com.topcoder.or.repository;

import javax.annotation.PostConstruct;

import com.topcoder.onlinereview.component.id.DBHelper;
import com.topcoder.onlinereview.component.id.IDGenerator;
import com.topcoder.onlinereview.component.search.SearchBundle;
import com.topcoder.onlinereview.component.search.SearchBundleManager;
import com.topcoder.onlinereview.grpc.review.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
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
        dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ReviewProto.Builder builder = ReviewProto.newBuilder();
            return builder.build();
        }, request.getReviewIdsList().toArray());
        responseObserver.onNext(GetReviewsResponse.newBuilder().setCount(result).build());
        responseObserver.onCompleted();
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
}
