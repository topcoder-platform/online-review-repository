package com.topcoder.or.repository;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.grpc.deliverable.proto.*;
import com.topcoder.onlinereview.component.search.SearchBundle;
import com.topcoder.onlinereview.component.search.SearchBundleManager;
import com.topcoder.onlinereview.component.search.filter.Filter;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;
import com.topcoder.or.util.SearchBundleHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.util.SerializationUtils;

@GrpcService
public class DeliverableService extends DeliverableServiceGrpc.DeliverableServiceImplBase {
    private final DBAccessor dbAccessor;
    private final SearchBundleManager searchBundleManager;

    public static final String DELIVERABLE_SEARCH_BUNDLE_NAME = "Deliverable Search Bundle";
    public static final String DELIVERABLE_WITH_SUBMISSIONS_SEARCH_BUNDLE_NAME = "Deliverable With Submission Search Bundle";
    private static final String KEY_NON_RESTRICTED_SB_NAME = "Non-restricted Late Deliverable Search Bundle";
    private static final String KEY_RESTRICTED_SB_NAME = "Restricted Late Deliverable Search Bundle";

    private SearchBundle deliverableSearchBundle;
    private SearchBundle deliverableWithSubmissionsSearchBundle;
    private SearchBundle nonRestrictedSearchBundle;
    private SearchBundle restrictedSearchBundle;

    public DeliverableService(DBAccessor dbAccessor, SearchBundleManager searchBundleManager) {
        this.dbAccessor = dbAccessor;
        this.searchBundleManager = searchBundleManager;
    }

    @PostConstruct
    public void postRun() {
        deliverableSearchBundle = searchBundleManager.getSearchBundle(DELIVERABLE_SEARCH_BUNDLE_NAME);
        deliverableWithSubmissionsSearchBundle = searchBundleManager
                .getSearchBundle(DELIVERABLE_WITH_SUBMISSIONS_SEARCH_BUNDLE_NAME);
        nonRestrictedSearchBundle = searchBundleManager.getSearchBundle(KEY_NON_RESTRICTED_SB_NAME);
        restrictedSearchBundle = searchBundleManager.getSearchBundle(KEY_RESTRICTED_SB_NAME);
        SearchBundleHelper.setSearchableFields(
                deliverableSearchBundle, SearchBundleHelper.DELIVERABLE_SEARCH_BUNDLE);
        SearchBundleHelper.setSearchableFields(deliverableWithSubmissionsSearchBundle,
                SearchBundleHelper.DELIVERABLE_WITH_SUBMISSIONS_SEARCH_BUNDLE);
    }

    @Override
    public void loadDeliverablesWithoutSubmission(LoadDeliverablesWithoutSubmissionRequest request,
            StreamObserver<LoadDeliverablesWithoutSubmissionResponse> responseObserver) {
        validateLoadDeliverablesWithoutSubmissionRequest(request);
        String sql = """
                SELECT r.project_id, p.project_phase_id, r.resource_id, d.required, d.deliverable_id, d.create_user, d.create_date, d.modify_user, d.modify_date, d.name, d.description
                FROM deliverable_lu d
                INNER JOIN resource r ON r.resource_role_id = d.resource_role_id
                INNER JOIN project_phase p ON p.project_id = r.project_id AND p.phase_type_id = d.phase_type_id
                WHERE d.submission_type_id IS NULL AND %s
                    """
                .formatted(constructSQLCondition(
                        request.getDeliverableIdsList(), request.getResourceIdsList(), request.getPhaseIdsList(),
                        null));
        List<DeliverableWithoutSubmissionProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            DeliverableWithoutSubmissionProto.Builder builder = DeliverableWithoutSubmissionProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setProjectPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setResourceId);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setRequired);
            ResultSetHelper.applyResultSetLong(rs, 5, builder::setDeliverableId);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 7, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 9, builder::setModifyDate);
            ResultSetHelper.applyResultSetString(rs, 10, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 11, builder::setDescription);
            return builder.build();
        });
        responseObserver.onNext(LoadDeliverablesWithoutSubmissionResponse.newBuilder()
                .addAllDeliverablesWithoutSubmissions(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadDeliverablesWithSubmission(LoadDeliverablesWithSubmissionRequest request,
            StreamObserver<LoadDeliverablesWithSubmissionResponse> responseObserver) {
        validateLoadDeliverablesWithSubmissionRequest(request);
        String sql = """
                SELECT u.project_id, p.project_phase_id, r.resource_id, s.submission_id, d.required, d.deliverable_id, d.create_user, d.create_date, d.modify_user, d.modify_date, d.name, d.description
                FROM deliverable_lu d
                INNER JOIN resource r ON r.resource_role_id = d.resource_role_id
                INNER JOIN project_phase p ON p.project_id = r.project_id AND p.phase_type_id = d.phase_type_id
                INNER JOIN upload u ON u.project_id = r.project_id and u.upload_status_id=1 and u.upload_type_id=1
                INNER JOIN submission s ON s.submission_type_id = d.submission_type_id AND s.submission_status_id = 1 and s.upload_id = u.upload_id
                WHERE d.submission_type_id IS NOT NULL AND u.create_date = (CASE WHEN p.phase_type_id = 18 THEN
                (SELECT MIN(u1.create_date) FROM upload u1 INNER JOIN submission s1 ON s1.upload_id = u1.upload_id AND s1.submission_status_id = 1 WHERE s1.submission_type_id = d.submission_type_id AND u1.upload_status_id = 1 AND u1.upload_type_id = 1 AND u1.project_id = p.project_id)
                ELSE u.create_date END ) AND %s
                    """
                .formatted(constructSQLCondition(
                        request.getDeliverableIdsList(), request.getResourceIdsList(), request.getPhaseIdsList(),
                        request.getSubmissionIdsList()));
        List<DeliverableWithSubmissionProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            DeliverableWithSubmissionProto.Builder builder = DeliverableWithSubmissionProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setProjectPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setSubmissionId);
            ResultSetHelper.applyResultSetBool(rs, 5, builder::setRequired);
            ResultSetHelper.applyResultSetLong(rs, 6, builder::setDeliverableId);
            ResultSetHelper.applyResultSetString(rs, 7, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 8, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 9, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 10, builder::setModifyDate);
            ResultSetHelper.applyResultSetString(rs, 11, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 12, builder::setDescription);
            return builder.build();
        });
        responseObserver.onNext(LoadDeliverablesWithSubmissionResponse.newBuilder()
                .addAllDeliverablesWithSubmissions(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateLateDeliverable(UpdateLateDeliverableRequest request,
            StreamObserver<UpdatedCountProto> responseObserver) {
        validateUpdateLateDeliverableRequest(request);
        String sql = """
                UPDATE late_deliverable SET project_phase_id = ?,
                resource_id = ?, deliverable_id = ?, deadline = ?, compensated_deadline = ?, create_date = ?,
                forgive_ind = ?, last_notified = ?, delay = ?, explanation = ?, explanation_date = ?, response = ?,
                response_user = ?, response_date = ?, late_deliverable_type_id = ? WHERE late_deliverable_id = ?
                    """;
        final Long projectPhaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        final Long deliverableId = Helper.extract(request::hasDeliverableId, request::getDeliverableId);
        final Date deadline = Helper.extractDate(request::hasDeadline, request::getDeadline);
        final Date compensatedDeadline = Helper.extractDate(request::hasCompensatedDeadline,
                request::getCompensatedDeadline);
        final Date createDate = Helper.extractDate(request::hasCreateDate, request::getCreateDate);
        final Boolean forgiveInd = Helper.extract(request::hasForgiveInd, request::getForgiveInd);
        final Date lastNotified = Helper.extractDate(request::hasLastNotified, request::getLastNotified);
        final Long delay = Helper.extract(request::hasDelay, request::getDelay);
        final String explanation = Helper.extract(request::hasExplanation, request::getExplanation);
        final Date explanationDate = Helper.extractDate(request::hasExplanationDate, request::getExplanationDate);
        final String response = Helper.extract(request::hasResponse, request::getResponse);
        final String responseUser = Helper.extract(request::hasResponseUser, request::getResponseUser);
        final Date responseDate = Helper.extractDate(request::hasResponseDate, request::getResponseDate);
        final Long lateDeliverableTypeId = Helper.extract(request::hasLateDeliverableTypeId,
                request::getLateDeliverableTypeId);
        final Long lateDeliverableId = Helper.extract(request::hasLateDeliverableId, request::getLateDeliverableId);
        final int updated = dbAccessor.executeUpdate(sql, projectPhaseId, resourceId, deliverableId, deadline,
                compensatedDeadline,
                createDate, forgiveInd, lastNotified, delay, explanation, explanationDate, response, responseUser,
                responseDate, lateDeliverableTypeId, lateDeliverableId);
        responseObserver.onNext(UpdatedCountProto.newBuilder().setCount(updated).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getLateDeliverableTypes(Empty request, StreamObserver<LateDeliverableTypesResponse> responseObserver) {
        String sql = """
                SELECT late_deliverable_type_id, name, description
                FROM late_deliverable_type_lu
                    """;
        List<LateDeliverableTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            LateDeliverableTypeProto.Builder builder = LateDeliverableTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setLateDeliverableTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setDescription);
            return builder.build();
        });
        responseObserver.onNext(LateDeliverableTypesResponse.newBuilder().addAllLateDeliverableTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void aggregationDeliverableCheck(AggregationDeliverableCheckRequest request,
            StreamObserver<AggregationDeliverableCheckResponse> responseObserver) {
        validateAggregationDeliverableCheckRequest(request);
        String sql = """
                SELECT modify_date
                FROM review
                WHERE committed = 1 AND resource_id = ?
                    """;
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        List<ModifyDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ModifyDateProto.Builder builder = ModifyDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            return builder.build();
        }, resourceId);
        responseObserver.onNext(AggregationDeliverableCheckResponse.newBuilder().addAllModifyDates(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void appealResponsesDeliverableCheck(AppealResponsesDeliverableCheckRequest request,
            StreamObserver<AppealResponsesDeliverableCheckResponse> responseObserver) {
        validateAppealResponsesDeliverableCheckRequest(request);
        String sql = """
                SELECT appeal_response_comment.modify_date
                FROM review_item_comment appeal_comment
                INNER JOIN comment_type_lu appeal_comment_type ON appeal_comment.comment_type_id = appeal_comment_type.comment_type_id AND appeal_comment_type.name = 'Appeal'
                INNER JOIN review_item ON appeal_comment.review_item_id = review_item.review_item_id
                INNER JOIN review ON review_item.review_id = review.review_id AND review.resource_id = ? AND review.submission_id = ?
                LEFT OUTER JOIN (review_item_comment appeal_response_comment INNER JOIN comment_type_lu appeal_response_comment_type ON appeal_response_comment.comment_type_id = appeal_response_comment_type.comment_type_id AND appeal_response_comment_type.name = 'Appeal Response') ON appeal_response_comment.review_item_id = appeal_comment.review_item_id
                        """;
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        final Long submissionId = Helper.extract(request::hasSubmissionId, request::getSubmissionId);
        List<ModifyDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ModifyDateProto.Builder builder = ModifyDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            return builder.build();
        }, resourceId, submissionId);
        responseObserver.onNext(AppealResponsesDeliverableCheckResponse.newBuilder().addAllModifyDates(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void committedReviewDeliverableCheck(CommittedReviewDeliverableCheckRequest request,
            StreamObserver<CommittedReviewDeliverableCheckResponse> responseObserver) {
        validateCommittedReviewDeliverableCheckRequest(request);
        String sql = """
                SELECT modify_date FROM review WHERE committed = 1 AND resource_id = ? AND project_phase_id = ?
                """;
        final Boolean isPerSubmission = Helper.extract(request::hasIsPerSubmission, request::getIsPerSubmission);
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        final Long projectPhaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        final Long submissionId = Helper.extract(request::hasSubmissionId, request::getSubmissionId);
        List<Object> params = new ArrayList<>() {
            {
                add(resourceId);
                add(resourceId);
                add(projectPhaseId);
            }
        };
        if (isPerSubmission) {
            sql = sql + " AND submission_id = ?";
            params.add(submissionId);
        }
        List<ModifyDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ModifyDateProto.Builder builder = ModifyDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            return builder.build();
        }, params.toArray());
        responseObserver.onNext(CommittedReviewDeliverableCheckResponse.newBuilder().addAllModifyDates(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void finalFixesDeliverableCheck(FinalFixesDeliverableCheckRequest request,
            StreamObserver<FinalFixesDeliverableCheckResponse> responseObserver) {
        validateFinalFixesDeliverableCheckRequest(request);
        String sql = """
                SELECT MAX(upload.modify_date) as modify_date FROM upload
                INNER JOIN upload_type_lu ON upload.upload_type_id = upload_type_lu.upload_type_id
                WHERE upload_type_lu.name = 'Final Fix'
                AND upload.project_phase_id = ? AND upload.resource_id = ?
                """;
        final Long projectPhaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        List<ModifyDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ModifyDateProto.Builder builder = ModifyDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            return builder.build();
        }, projectPhaseId, resourceId);
        responseObserver.onNext(FinalFixesDeliverableCheckResponse.newBuilder().addAllModifyDates(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void finalReviewDeliverableCheck(FinalReviewDeliverableCheckRequest request,
            StreamObserver<FinalReviewDeliverableCheckResponse> responseObserver) {
        validateFinalReviewDeliverableCheckRequest(request);
        String sql = """
                SELECT MAX(review_comment.modify_date) as modify_date, review.submission_id FROM review_comment
                INNER JOIN comment_type_lu ON review_comment.comment_type_id = comment_type_lu.comment_type_id
                INNER JOIN review ON review.review_id = review_comment.review_id
                WHERE review_comment.resource_id = ?
                AND comment_type_lu.name = 'Final Review Comment'
                AND (review_comment.extra_info = 'Approved' OR review_comment.extra_info = 'Rejected')
                GROUP BY review.submission_id
                """;
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        List<FinalReviewDeliverableCheckResponseMessage> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            FinalReviewDeliverableCheckResponseMessage.Builder builder = FinalReviewDeliverableCheckResponseMessage
                    .newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setSubmissionId);
            return builder.build();
        }, resourceId);
        responseObserver.onNext(FinalReviewDeliverableCheckResponse.newBuilder().addAllResults(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void individualReviewDeliverableCheck(IndividualReviewDeliverableCheckRequest request,
            StreamObserver<IndividualReviewDeliverableCheckResponse> responseObserver) {
        validateIndividualReviewDeliverableCheckRequest(request);
        String sql = """
                SELECT review.modify_date, resource_submission.submission_id
                FROM resource_submission
                LEFT JOIN review ON review.resource_id = resource_submission.resource_id
                AND review.submission_id = resource_submission.submission_id AND committed = 1
                WHERE resource_submission.resource_id = ?
                """;
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        List<IndividualReviewDeliverableCheckResponseMessage> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            IndividualReviewDeliverableCheckResponseMessage.Builder builder = IndividualReviewDeliverableCheckResponseMessage
                    .newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setSubmissionId);
            return builder.build();
        }, resourceId);
        responseObserver.onNext(IndividualReviewDeliverableCheckResponse.newBuilder().addAllResults(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void specificationSubmissionDeliverableCheck(SpecificationSubmissionDeliverableCheckRequest request,
            StreamObserver<SpecificationSubmissionDeliverableCheckResponse> responseObserver) {
        validateSpecificationSubmissionDeliverableCheckRequest(request);
        String sql = """
                SELECT s.modify_date
                FROM submission s
                INNER JOIN upload u ON s.upload_id = u.upload_id
                WHERE s.submission_status_id <> 5
                AND s.submission_type_id = 2
                AND u.resource_id = ?
                AND u.project_phase_id = ?
                """;
        final Long projectPhaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        List<ModifyDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ModifyDateProto.Builder builder = ModifyDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            return builder.build();
        }, resourceId, projectPhaseId);
        responseObserver
                .onNext(SpecificationSubmissionDeliverableCheckResponse.newBuilder().addAllModifyDates(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void submissionDeliverableCheck(SubmissionDeliverableCheckRequest request,
            StreamObserver<SubmissionDeliverableCheckResponse> responseObserver) {
        validateSubmissionDeliverableCheckRequest(request);
        String sql = """
                SELECT MAX(upload.modify_date) as modify_date FROM upload
                INNER JOIN upload_type_lu ON upload.upload_type_id = upload_type_lu.upload_type_id
                INNER JOIN upload_status_lu ON upload.upload_status_id = upload_status_lu.upload_status_id
                LEFT JOIN submission ON upload.upload_id = submission.upload_id
                WHERE upload_type_lu.name = 'Submission' AND upload_status_lu.name = 'Active'
                AND upload.resource_id = ? AND submission.submission_type_id = ?
                """;
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        final Long submissionTypeId = Helper.extract(request::hasSubmissionTypeId, request::getSubmissionTypeId);
        List<ModifyDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ModifyDateProto.Builder builder = ModifyDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            return builder.build();
        }, resourceId, submissionTypeId);
        responseObserver
                .onNext(SubmissionDeliverableCheckResponse.newBuilder().addAllModifyDates(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void submitterCommentDeliverableCheck(SubmitterCommentDeliverableCheckRequest request,
            StreamObserver<SubmitterCommentDeliverableCheckResponse> responseObserver) {
        validateSubmitterCommentDeliverableCheckRequest(request);
        String sql = """
                SELECT MAX(review_comment.modify_date) as modify_date, review.submission_id FROM review_comment
                INNER JOIN comment_type_lu ON review_comment.comment_type_id = comment_type_lu.comment_type_id
                INNER JOIN review ON review.review_id = review_comment.review_id
                INNER JOIN resource ON review.resource_id = resource.resource_id
                INNER JOIN phase_dependency ON resource.project_phase_id = phase_dependency.dependency_phase_id
                WHERE review_comment.resource_id = ?
                AND phase_dependency.dependent_phase_id = ?
                AND comment_type_lu.name = 'Submitter Comment'
                AND (review_comment.extra_info = 'Approved' OR review_comment.extra_info = 'Rejected')
                GROUP BY review.submission_id
                """;
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        final Long projectPhaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        List<SubmitterCommentDeliverableCheckResponseMessage> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            SubmitterCommentDeliverableCheckResponseMessage.Builder builder = SubmitterCommentDeliverableCheckResponseMessage
                    .newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setSubmissionId);
            return builder.build();
        }, resourceId, projectPhaseId);
        responseObserver.onNext(SubmitterCommentDeliverableCheckResponse.newBuilder().addAllResults(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void testCasesDeliverableCheck(TestCasesDeliverableCheckRequest request,
            StreamObserver<TestCasesDeliverableCheckResponse> responseObserver) {
        validateTestCasesDeliverableCheckRequest(request);
        String sql = """
                SELECT MAX(upload.modify_date) as modify_date
                FROM upload
                INNER JOIN upload_type_lu ON upload.upload_type_id = upload_type_lu.upload_type_id
                INNER JOIN upload_status_lu ON upload.upload_status_id = upload_status_lu.upload_status_id
                WHERE upload_type_lu.name = 'Test Case' AND upload_status_lu.name = 'Active'
                AND upload.resource_id = ?
                """;
        final Long resourceId = Helper.extract(request::hasResourceId, request::getResourceId);
        List<ModifyDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ModifyDateProto.Builder builder = ModifyDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setModifyDate);
            return builder.build();
        }, resourceId);
        responseObserver.onNext(TestCasesDeliverableCheckResponse.newBuilder().addAllModifyDates(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchDeliverables(FilterProto request, StreamObserver<SearchDeliverablesResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<SearchDeliverablesProto> result = deliverableSearchBundle.search(filter, (rs, _i) -> {
            SearchDeliverablesProto.Builder builder = SearchDeliverablesProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setDeliverableId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setProjectPhaseId);
            return builder.build();
        });
        responseObserver.onNext(SearchDeliverablesResponse.newBuilder().addAllDeliverables(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchDeliverablesWithSubmission(FilterProto request,
            StreamObserver<SearchDeliverablesWithSubmissionResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<SearchDeliverablesWithSubmissionProto> result = deliverableWithSubmissionsSearchBundle.search(filter,
                (rs, _i) -> {
                    SearchDeliverablesWithSubmissionProto.Builder builder = SearchDeliverablesWithSubmissionProto
                            .newBuilder();
                    ResultSetHelper.applyResultSetLong(rs, 1, builder::setDeliverableId);
                    ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceId);
                    ResultSetHelper.applyResultSetLong(rs, 3, builder::setProjectPhaseId);
                    ResultSetHelper.applyResultSetLong(rs, 4, builder::setSubmissionId);
                    return builder.build();
                });
        responseObserver
                .onNext(SearchDeliverablesWithSubmissionResponse.newBuilder().addAllDeliverables(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchLateDeliverablesNonRestricted(FilterProto request,
            StreamObserver<SearchLateDeliverablesResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<LateDeliverablesProto> result = nonRestrictedSearchBundle.search(filter,
                (rs, _i) -> {
                    return loadLateDeliverablesFromSearch(rs);
                });
        responseObserver
                .onNext(SearchLateDeliverablesResponse.newBuilder().addAllLateDeliverables(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchLateDeliverablesRestricted(FilterProto request,
            StreamObserver<SearchLateDeliverablesResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<LateDeliverablesProto> result = restrictedSearchBundle.search(filter,
                (rs, _i) -> {
                    return loadLateDeliverablesFromSearch(rs);
                });
        responseObserver
                .onNext(SearchLateDeliverablesResponse.newBuilder().addAllLateDeliverables(result).build());
        responseObserver.onCompleted();
    }

    /**
     * Constructs WHERE clause of the SQL statement for retrieving deliverables.
     *
     * @param deliverableIds The ids of deliverables to load, should not be null
     * @param resourceIds    The resource ids of deliverables to load, should not be
     *                       null
     * @param phaseIds       The phase ids of deliverables to load, should not be
     *                       null
     * @param submissionIds  The ids of the submission for each deliverable, can be
     *                       null
     * @return SQL WHERE clause
     */
    private String constructSQLCondition(
            List<Long> deliverableIds, List<Long> resourceIds, List<Long> phaseIds, List<Long> submissionIds) {
        Set<Long> distinctDeliverableIds = new HashSet<>();
        for (Long deliverableId : deliverableIds) {
            distinctDeliverableIds.add(deliverableId);
        }

        // build the match condition string.
        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append('(');

        // To reduce size of the SQL we move the equality check for deliverable_id out
        // of the braces.
        // We do that by several linear traversal through the arrays each time picking
        // up the only items
        // with
        // a certain deliverable ID.
        boolean firstDeliverable = true;
        for (long deliverableId : distinctDeliverableIds) {
            if (!firstDeliverable) {
                stringBuffer.append(" OR ");
            }
            firstDeliverable = false;
            stringBuffer.append("(d.deliverable_id=").append(deliverableId).append(" AND (");

            // To reduce size of the SQL even further, we now group by phase ID.
            Map<Long, List<Long>> submissionsByPhase = new HashMap<Long, List<Long>>();
            Map<Long, List<Long>> resourcesByPhase = new HashMap<Long, List<Long>>();
            for (int i = 0; i < deliverableIds.size(); ++i) {
                if (deliverableIds.get(i) == deliverableId) {
                    List<Long> resources = resourcesByPhase.get(phaseIds.get(i));
                    if (resources == null) {
                        resources = new ArrayList<Long>();
                        resourcesByPhase.put(phaseIds.get(i), resources);
                    }
                    resources.add(resourceIds.get(i));

                    if (submissionIds != null) {
                        List<Long> submissions = submissionsByPhase.get(phaseIds.get(i));
                        if (submissions == null) {
                            submissions = new ArrayList<Long>();
                            submissionsByPhase.put(phaseIds.get(i), submissions);
                        }
                        submissions.add(submissionIds.get(i));
                    }
                }
            }

            // Now loop through all phases and for each phase construct a separate block of
            // conditions
            // separated by OR.
            boolean firstPhase = true;
            for (Long phaseId : resourcesByPhase.keySet()) {
                if (!firstPhase) {
                    stringBuffer.append(" OR ");
                }
                firstPhase = false;
                stringBuffer.append("(p.project_phase_id=").append(phaseId).append(" AND (");

                List<Long> resources = resourcesByPhase.get(phaseId);
                List<Long> submissions = submissionsByPhase.get(phaseId);
                for (int i = 0; i < resources.size(); ++i) {
                    if (i > 0) {
                        stringBuffer.append(" OR ");
                    }

                    stringBuffer.append("(");
                    if (submissions != null) {
                        stringBuffer.append("s.submission_id=").append(submissions.get(i)).append(" AND ");
                    }
                    stringBuffer.append("r.resource_id=").append(resources.get(i)).append(")");
                }

                stringBuffer.append("))");
            }

            stringBuffer.append("))");
        }

        stringBuffer.append(')');
        return stringBuffer.toString();
    }

    private void validateLoadDeliverablesWithoutSubmissionRequest(LoadDeliverablesWithoutSubmissionRequest request) {
        Helper.assertObjectNotEmpty(request::getDeliverableIdsCount, "deliverable_ids");
        Helper.assertObjectNotEmpty(request::getPhaseIdsCount, "phase_ids");
        Helper.assertObjectNotEmpty(request::getResourceIdsCount, "resource_ids");
        if (request.getDeliverableIdsCount() != request.getResourceIdsCount()
                || request.getDeliverableIdsCount() != request.getPhaseIdsCount()) {
            throw new IllegalArgumentException(
                    "deliverableIds, resourceIds and phaseIds should have the same number of elements.");
        }
    }

    private void validateLoadDeliverablesWithSubmissionRequest(LoadDeliverablesWithSubmissionRequest request) {
        Helper.assertObjectNotEmpty(request::getDeliverableIdsCount, "deliverable_ids");
        Helper.assertObjectNotEmpty(request::getPhaseIdsCount, "phase_ids");
        Helper.assertObjectNotEmpty(request::getResourceIdsCount, "resource_ids");
        Helper.assertObjectNotEmpty(request::getSubmissionIdsCount, "submission_ids");
        if (request.getDeliverableIdsCount() != request.getResourceIdsCount()
                || request.getDeliverableIdsCount() != request.getPhaseIdsCount()
                || request.getDeliverableIdsCount() != request.getSubmissionIdsCount()) {
            throw new IllegalArgumentException(
                    "deliverableIds, resourceIds, phaseIds and submissionIds should have the same number of elements.");
        }
    }

    private void validateUpdateLateDeliverableRequest(UpdateLateDeliverableRequest request) {
        Helper.assertObjectNotNull(request::hasLateDeliverableId, "late_deliverable_id");
        Helper.assertObjectNotNull(request::hasLateDeliverableTypeId, "late_deliverable_type_id");
        Helper.assertObjectNotNull(request::hasProjectPhaseId, "project_phase_id");
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasDeliverableId, "deliverable_id");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasForgiveInd, "forgive_ind");
    }

    private void validateAggregationDeliverableCheckRequest(AggregationDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private void validateAppealResponsesDeliverableCheckRequest(AppealResponsesDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasSubmissionId, "submission_id");
    }

    private void validateCommittedReviewDeliverableCheckRequest(CommittedReviewDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasIsPerSubmission, "is_per_submission");
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasProjectPhaseId, "project_phase_id");
        if (request.getIsPerSubmission()) {
            Helper.assertObjectNotNull(request::hasSubmissionId, "submission_id");
        }
    }

    private void validateFinalFixesDeliverableCheckRequest(FinalFixesDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasProjectPhaseId, "project_phase_id");
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private void validateFinalReviewDeliverableCheckRequest(FinalReviewDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private void validateIndividualReviewDeliverableCheckRequest(IndividualReviewDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private void validateSpecificationSubmissionDeliverableCheckRequest(
            SpecificationSubmissionDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasProjectPhaseId, "project_phase_id");
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private void validateSubmissionDeliverableCheckRequest(SubmissionDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasSubmissionTypeId, "submission_type_id");
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private void validateSubmitterCommentDeliverableCheckRequest(SubmitterCommentDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasProjectPhaseId, "project_phase_id");
    }

    private void validateTestCasesDeliverableCheckRequest(TestCasesDeliverableCheckRequest request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private LateDeliverablesProto loadLateDeliverablesFromSearch(ResultSet rs) throws SQLException {
        LateDeliverablesProto.Builder builder = LateDeliverablesProto
                .newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "late_deliverable_id", builder::setLateDeliverableId);
        ResultSetHelper.applyResultSetLong(rs, "project_id", builder::setProjectId);
        ResultSetHelper.applyResultSetLong(rs, "project_phase_id", builder::setProjectPhaseId);
        ResultSetHelper.applyResultSetLong(rs, "resource_id", builder::setResourceId);
        ResultSetHelper.applyResultSetLong(rs, "deliverable_id", builder::setDeliverableId);
        ResultSetHelper.applyResultSetTimestamp(rs, "deadline", builder::setDeadline);
        ResultSetHelper.applyResultSetTimestamp(rs, "compensated_deadline",
                builder::setCompensatedDeadline);
        ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
        ResultSetHelper.applyResultSetBool(rs, "forgive_ind", builder::setForgiveInd);
        ResultSetHelper.applyResultSetTimestamp(rs, "last_notified", builder::setLastNotified);
        ResultSetHelper.applyResultSetLong(rs, "delay", builder::setDelay);
        ResultSetHelper.applyResultSetString(rs, "explanation", builder::setExplanation);
        ResultSetHelper.applyResultSetTimestamp(rs, "explanation_date", builder::setExplanationDate);
        ResultSetHelper.applyResultSetString(rs, "response", builder::setResponse);
        ResultSetHelper.applyResultSetString(rs, "response_user", builder::setResponseUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "response_date", builder::setResponseDate);
        ResultSetHelper.applyResultSetLong(rs, "late_deliverable_type_id",
                builder::setLateDeliverableTypeId);
        ResultSetHelper.applyResultSetString(rs, "name", builder::setName);
        ResultSetHelper.applyResultSetString(rs, "description", builder::setDescription);
        return builder.build();
    }
}