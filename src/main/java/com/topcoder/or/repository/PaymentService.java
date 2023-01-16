package com.topcoder.or.repository;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.util.SerializationUtils;

import com.topcoder.onlinereview.component.search.SearchBundle;
import com.topcoder.onlinereview.component.search.SearchBundleManager;
import com.topcoder.onlinereview.component.search.filter.Filter;
import com.topcoder.onlinereview.grpc.payment.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class PaymentService extends PaymentServiceGrpc.PaymentServiceImplBase {
    private final DBAccessor dbAccessor;
    private final SearchBundleManager searchBundleManager;

    private static final String PAYMENT_SEARCH_BUNDLE_NAME = "ProjectPaymentSearchBundle";

    private SearchBundle searchBundle;

    public PaymentService(DBAccessor dbAccessor, SearchBundleManager searchBundleManager) {
        this.dbAccessor = dbAccessor;
        this.searchBundleManager = searchBundleManager;
    }

    @PostConstruct
    public void postRun() {
        searchBundle = searchBundleManager.getSearchBundle(PAYMENT_SEARCH_BUNDLE_NAME);
    }

    @Override
    public void createPayment(CreatePaymentRequest request, StreamObserver<IdProto> responseObserver) {
        validateCreatePaymentRequest(request);
        String sql = """
                INSERT INTO project_payment
                (resource_id, submission_id, amount, pacts_payment_id, create_user, create_date, modify_user, modify_date, project_payment_type_id)
                VALUES (?, ?, ?, ?, ?, CURRENT, ?, CURRENT, ?)
                """;
        ProjectPaymentProto p = request.getProjectPayment();
        final BigDecimal amount = Helper.extractBigDecimal(p::hasAmount, p::getAmount);
        final Long pactPaymentId = Helper.extract(p::hasPactsPaymentId, p::getPactsPaymentId);
        final Long submissionId = Helper.extract(p::hasSubmissionId, p::getSubmissionId);
        Number id = dbAccessor.executeUpdateReturningKey(sql, conn -> {
            PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, p.getResourceId());
            ps.setLong(2, submissionId);
            ps.setBigDecimal(3, amount);
            ps.setLong(4, pactPaymentId);
            ps.setString(5, request.getOperator());
            ps.setString(6, request.getOperator());
            return ps;
        });
        responseObserver.onNext(IdProto.newBuilder().setId(id.longValue()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePayment(UpdatePaymentRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdatePaymentRequest(request);
        String sql = """
                UPDATE project_payment set project_payment_type_id = ?, resource_id = ?, submission_id = ?, amount = ?,
                pacts_payment_id = ?, modify_user = ?, modify_date = CURRENT
                WHERE project_payment_id = ?
                """;
        ProjectPaymentProto p = request.getProjectPayment();
        final BigDecimal amount = Helper.extractBigDecimal(p::hasAmount, p::getAmount);
        final Long pactPaymentId = Helper.extract(p::hasPactsPaymentId, p::getPactsPaymentId);
        final Long submissionId = Helper.extract(p::hasSubmissionId, p::getSubmissionId);
        int affected = dbAccessor.executeUpdate(sql, p.getProjectPaymentType().getId(), p.getResourceId(), submissionId,
                amount, pactPaymentId, request.getOperator(), p.getId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPayment(IdProto request, StreamObserver<ProjectPaymentProto> responseObserver) {
        validateIdProto(request);
        String sql = """
                SELECT pp.project_payment_id, pp.resource_id, pp.submission_id, pp.amount, pp.pacts_payment_id, pp.create_user, pp.create_date,
                pp.modify_user, pp.modify_date, ppt.project_payment_type_id, ppt.name, ppt.mergeable
                FROM project_payment pp
                LEFT JOIN project_payment_type_lu ppt on pp.project_payment_type_id = ppt.project_payment_type_id
                WHERE pp.project_payment_id = ?
                """;
        List<ProjectPaymentProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectPaymentProto.Builder builder = ProjectPaymentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setSubmissionId);
            ResultSetHelper.applyResultSetBigDecimal(rs, 4, builder::setAmount);
            ResultSetHelper.applyResultSetLong(rs, 5, builder::setPactsPaymentId);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 7, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 9, builder::setModifyDate);
            ProjectPaymentTypeProto.Builder pBuilder = ProjectPaymentTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 10, pBuilder::setId);
            ResultSetHelper.applyResultSetString(rs, 11, pBuilder::setName);
            ResultSetHelper.applyResultSetBool(rs, 12, pBuilder::setMergeable);
            builder.setProjectPaymentType(pBuilder.build());
            return builder.build();
        }, request.getId());
        responseObserver.onNext(result.isEmpty() ? ProjectPaymentProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void deletePayment(IdProto request, StreamObserver<CountProto> responseObserver) {
        validateIdProto(request);
        String sql = """
                DELETE FROM project_payment WHERE project_payment_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createPaymentAdjustment(ProjectPaymentAdjustmentProto request,
            StreamObserver<CountProto> responseObserver) {
        validateProjectPaymentAdjustmentProto(request);
        String sql = """
                INSERT INTO project_payment_adjustment (project_id, resource_role_id, fixed_amount, multiplier) VALUES (?, ?, ?, ?)
                """;
        final BigDecimal fixedAmount = Helper.extractBigDecimal(request::hasFixedAmount, request::getFixedAmount);
        final Double multiplier = Helper.extract(request::hasMultiplier, request::getMultiplier);
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getResourceRoleId(), fixedAmount,
                multiplier);
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePaymentAdjustment(ProjectPaymentAdjustmentProto request,
            StreamObserver<CountProto> responseObserver) {
        validateProjectPaymentAdjustmentProto(request);
        String sql = """
                UPDATE project_payment_adjustment SET fixed_amount = ?, multiplier = ? WHERE project_id = ? AND resource_role_id = ?
                    """;
        final BigDecimal fixedAmount = Helper.extractBigDecimal(request::hasFixedAmount, request::getFixedAmount);
        final Double multiplier = Helper.extract(request::hasMultiplier, request::getMultiplier);
        int affected = dbAccessor.executeUpdate(sql, fixedAmount, multiplier, request.getProjectId(),
                request.getResourceRoleId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPaymentAdjustments(IdProto request, StreamObserver<GetPaymentAdjustmentsResponse> responseObserver) {
        validateIdProto(request);
        String sql = """
                SELECT project_id, resource_role_id, fixed_amount, multiplier FROM project_payment_adjustment where project_id = ?
                """;
        List<ProjectPaymentAdjustmentProto> response = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectPaymentAdjustmentProto.Builder builder = ProjectPaymentAdjustmentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceRoleId);
            ResultSetHelper.applyResultSetBigDecimal(rs, 3, builder::setFixedAmount);
            ResultSetHelper.applyResultSetDouble(rs, 4, builder::setMultiplier);
            return builder.build();
        }, request.getId());
        responseObserver.onNext(GetPaymentAdjustmentsResponse.newBuilder().addAllPaymentAdjustments(response).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPaymentAdjustmentsForResource(GetPaymentAdjustmentsRequest request,
            StreamObserver<GetPaymentAdjustmentsResponse> responseObserver) {
        validateGetPaymentAdjustmentsRequest(request);
        String sql = """
                SELECT resource_role_id, fixed_amount, multiplier FROM project_payment_adjustment WHERE project_id=? AND resource_role_id IN (%s)
                    """;
        String inSql = Helper.getInClause(request.getResourceIdsCount());
        List<Object> param = new ArrayList<>();
        param.add(request.getProjectId());
        param.addAll(request.getResourceIdsList());
        List<ProjectPaymentAdjustmentProto> response = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ProjectPaymentAdjustmentProto.Builder builder = ProjectPaymentAdjustmentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setResourceRoleId);
            ResultSetHelper.applyResultSetBigDecimal(rs, 2, builder::setFixedAmount);
            ResultSetHelper.applyResultSetDouble(rs, 3, builder::setMultiplier);
            return builder.build();
        }, param.toArray());
        responseObserver.onNext(GetPaymentAdjustmentsResponse.newBuilder().addAllPaymentAdjustments(response).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isProjectPaymentTypeExists(IdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateIdProto(request);
        boolean result = checkEntityExists("project_payment_type_lu", "project_payment_type_id", request.getId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isResourceExists(IdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateIdProto(request);
        boolean result = checkEntityExists("resource", "resource_id", request.getId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isSubmissionExists(IdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateIdProto(request);
        boolean result = checkEntityExists("submission", "submission_id", request.getId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDefaultPayments(GetDefaultPaymentsRequest request,
            StreamObserver<GetDefaultPaymentsResponse> responseObserver) {
        validateGetDefaultPaymentsRequest(request);
        String sql = """
                SELECT dpp.resource_role_id, dpp.fixed_amount, dpp.base_coefficient, dpp.incremental_coefficient,max(pr.prize_amount) as prize,
                sum(case when s.submission_type_id = 1 then 1 else 0 end) as total_contest_submissions,
                sum(case when s.submission_type_id = 1 and s.submission_status_id != 2 then 1 else 0 end) as passed_contest_submissions,
                sum(case when s.submission_type_id = 3 then 1 else 0 end) as total_checkpoint_submissions,
                sum(case when s.submission_type_id = 3 and s.submission_status_id != 6 then 1 else 0 end) as passed_checkpoint_submissions,
                sum(case when s.submission_type_id = 1 and exists (select 1 from review r where r.submission_id = s.submission_id and r.committed = 1) then 1 else 0 end) as total_reviewed_contest_submissions
                FROM default_project_payment dpp
                INNER JOIN project p ON dpp.project_category_id = p.project_category_id and p.project_id=?
                LEFT OUTER JOIN prize pr ON pr.project_id=p.project_id and pr.prize_type_id=15 and pr.place=1
                LEFT OUTER JOIN upload u ON u.project_id = p.project_id and u.upload_type_id = 1
                LEFT OUTER JOIN submission s ON s.submission_type_id in (1,3) and s.upload_id = u.upload_id and s.submission_status_id in (1,2,3,4,6,7)
                WHERE dpp.resource_role_id in (2,4,5,6,7,8,9,14,18,19,20,21)
                GROUP BY dpp.resource_role_id, dpp.fixed_amount, dpp.base_coefficient, dpp.incremental_coefficient
                """;
        List<DefaultPaymentProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            DefaultPaymentProto.Builder builder = DefaultPaymentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setResourceRoleId);
            ResultSetHelper.applyResultSetBigDecimal(rs, 2, builder::setFixedAmount);
            ResultSetHelper.applyResultSetFloat(rs, 3, builder::setBaseCoefficient);
            ResultSetHelper.applyResultSetFloat(rs, 4, builder::setIncrementalCoefficient);
            ResultSetHelper.applyResultSetFloat(rs, 5, builder::setPrize);
            ResultSetHelper.applyResultSetInt(rs, 6, builder::setTotalContestSubmissions);
            ResultSetHelper.applyResultSetInt(rs, 7, builder::setPassedContestSubmissions);
            ResultSetHelper.applyResultSetInt(rs, 8, builder::setTotalCheckpointSubmissions);
            ResultSetHelper.applyResultSetInt(rs, 9, builder::setPassedCheckpointSubmissions);
            ResultSetHelper.applyResultSetInt(rs, 10, builder::setTotalReviewedContestSubmissions);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(GetDefaultPaymentsResponse.newBuilder().addAllDefaultPayments(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDefaultPayment(GetDefaultPaymentRequest request,
            StreamObserver<GetDefaultPaymentResponse> responseObserver) {
        validateGetDefaultPaymentRequest(request);
        String sql = """
                SELECT fixed_amount, base_coefficient, incremental_coefficient
                FROM default_project_payment
                WHERE project_category_id = ? and resource_role_id = ?
                """;
        List<DefaultPaymentProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            DefaultPaymentProto.Builder builder = DefaultPaymentProto.newBuilder();
            ResultSetHelper.applyResultSetBigDecimal(rs, 1, builder::setFixedAmount);
            ResultSetHelper.applyResultSetFloat(rs, 2, builder::setBaseCoefficient);
            ResultSetHelper.applyResultSetFloat(rs, 3, builder::setIncrementalCoefficient);
            return builder.build();
        }, request.getProjectCategoryId(), request.getResourceRoleId());
        responseObserver.onNext(result.isEmpty() ? GetDefaultPaymentResponse.getDefaultInstance()
                : GetDefaultPaymentResponse.newBuilder().setDefaultPayment(result.get(0)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchPayments(FilterProto request, StreamObserver<SearchPaymentsResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<ProjectPaymentProto> result = searchBundle.search(filter, (rs, _i) -> {
            ProjectPaymentProto.Builder builder = ProjectPaymentProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, "project_payment_id", builder::setId);
            ResultSetHelper.applyResultSetLong(rs, "resource_id", builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, "submission_id", builder::setSubmissionId);
            ResultSetHelper.applyResultSetBigDecimal(rs, "amount", builder::setAmount);
            ResultSetHelper.applyResultSetLong(rs, "pacts_payment_id", builder::setPactsPaymentId);
            ResultSetHelper.applyResultSetString(rs, "create_user", builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, "modify_user", builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, "modify_date", builder::setModifyDate);
            ProjectPaymentTypeProto.Builder pBuilder = ProjectPaymentTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, "project_payment_type_id", pBuilder::setId);
            ResultSetHelper.applyResultSetString(rs, "name", pBuilder::setName);
            ResultSetHelper.applyResultSetBool(rs, "mergeable", pBuilder::setMergeable);
            ResultSetHelper.applyResultSetLong(rs, "pacts_payment_type_id", pBuilder::setPactsPaymentTypeId);
            builder.setProjectPaymentType(pBuilder.build());
            return builder.build();
        });
        responseObserver.onNext(SearchPaymentsResponse.newBuilder().addAllPayments(result).build());
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

    private void validateCreatePaymentRequest(CreatePaymentRequest request) {
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        Helper.assertObjectNotNull(request::hasProjectPayment, "project_payment");
        ProjectPaymentProto p = request.getProjectPayment();
        Helper.assertObjectNotNullAndPositive(p::hasResourceId, p::getResourceId, "resource_id");
        Helper.assertObjectNotNull(p::hasProjectPaymentType, "project_payment_type");
        ProjectPaymentTypeProto pp = p.getProjectPaymentType();
        Helper.assertObjectNotNullAndPositive(pp::hasId, p::getId, "project_payment_type_id");
    }

    private void validateUpdatePaymentRequest(UpdatePaymentRequest request) {
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        Helper.assertObjectNotNull(request::hasProjectPayment, "project_payment");
        ProjectPaymentProto p = request.getProjectPayment();
        Helper.assertObjectNotNullAndPositive(p::hasId, p::getId, "id");
        Helper.assertObjectNotNullAndPositive(p::hasResourceId, p::getResourceId, "resource_id");
        Helper.assertObjectNotNull(p::hasProjectPaymentType, "project_payment_type");
        ProjectPaymentTypeProto pp = p.getProjectPaymentType();
        Helper.assertObjectNotNullAndPositive(pp::hasId, p::getId, "project_payment_type_id");
    }

    private void validateProjectPaymentAdjustmentProto(ProjectPaymentAdjustmentProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasProjectId, request::getProjectId, "project_id");
        Helper.assertObjectNotNullAndPositive(request::hasResourceRoleId, request::getResourceRoleId,
                "resource_role_id");
    }

    private void validateIdProto(IdProto request) {
        Helper.assertObjectNotNull(request::hasId, "id");
    }

    private void validateGetPaymentAdjustmentsRequest(GetPaymentAdjustmentsRequest request) {
        Helper.assertObjectNotNullAndPositive(request::hasProjectId, request::getProjectId, "project_id");
        Helper.assertObjectNotEmpty(request::getResourceIdsCount, "resource_ids");
    }

    private void validateGetDefaultPaymentsRequest(GetDefaultPaymentsRequest request) {
        Helper.assertObjectNotNullAndPositive(request::hasProjectId, request::getProjectId, "project_id");
    }

    private void validateGetDefaultPaymentRequest(GetDefaultPaymentRequest request) {
        Helper.assertObjectNotNullAndPositive(request::hasProjectCategoryId, request::getProjectCategoryId,
                "project_category_id");
        Helper.assertObjectNotNullAndPositive(request::hasResourceRoleId, request::getResourceRoleId,
                "resource_role_id");
    }
}
