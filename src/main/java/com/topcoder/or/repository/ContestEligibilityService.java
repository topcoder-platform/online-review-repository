package com.topcoder.or.repository;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.grpc.contesteligibility.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.ArrayList;
import java.util.List;

@GrpcService
public class ContestEligibilityService extends ContestEligibilityServiceGrpc.ContestEligibilityServiceImplBase {
    private final DBAccessor dbAccessor;

    public ContestEligibilityService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
        validateCreateRequest(request);
        String sql = """
                insert into common_oltp:contest_eligibility(contest_eligibility_id, contest_id, is_studio)
                VALUES(common_oltp:CONTEST_ELIGIBILITY_SEQ.NEXTVAL, ?, ?)
                    """;
        final Long contestId = Helper.extract(request::hasContestId, request::getContestId);
        final Boolean isStudio = Helper.extract(request::hasStudio, request::getStudio);

        int inserted = dbAccessor.executeUpdate(sql, contestId, isStudio);
        if (inserted != 1) {
            responseObserver.onError(Status.INTERNAL.withDescription("Insert failed").asRuntimeException());
            return;
        }
        sql = """
                select max(contest_eligibility_id)
                from common_oltp:contest_eligibility
                where contest_id = ?
                    """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, contestId);
        CreateResponse.Builder rBuilder = CreateResponse.newBuilder();
        if (!result.isEmpty()) {
            rBuilder.setContestEligibilityId(result.get(0));
        }
        responseObserver.onNext(rBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<Empty> responseObserver) {
        validateRemoveRequest(request);
        String sql = """
                delete from common_oltp:contest_eligibility
                where contest_eligibility_id = ?
                    """;
        final Long contestEligibilityId = Helper.extract(request::hasContestEligibilityId,
                request::getContestEligibilityId);
        dbAccessor.executeUpdate(sql, contestEligibilityId);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void bulkRemove(BulkRemoveRequest request, StreamObserver<Empty> responseObserver) {
        validateBulkRemoveRequest(request);
        String sql = """
                delete from common_oltp:contest_eligibility
                where contest_eligibility_id in (%s)
                    """;
        String inSql = Helper.getInClause(request.getContestEligibilityIdsCount());
        dbAccessor.executeUpdate(sql.formatted(inSql), request.getContestEligibilityIdsList().toArray());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void update(UpdateRequest request, StreamObserver<Empty> responseObserver) {
        validateUpdateRequest(request);
        String sql = """
                update common_oltp:contest_eligibility
                set contest_id=?, is_studio=?
                where contest_eligibility_id=?
                        """;
        final Long contestId = Helper.extract(request::hasContestId, request::getContestId);
        final Boolean studio = Helper.extract(request::hasStudio, request::getStudio);
        final Long contestEligibilityId = Helper.extract(request::hasContestEligibilityId,
                request::getContestEligibilityId);
        dbAccessor.executeUpdate(sql, contestId, studio, contestEligibilityId);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getContestEligibility(GetContestEligibilityRequest request,
            StreamObserver<ContestEligibilitiesResponse> responseObserver) {
        validateGetContestEligibilityRequest(request);
        String sql = """
                select c.contest_eligibility_id, c.contest_id, c.is_studio, g.group_id
                from common_oltp:contest_eligibility AS c
                JOIN common_oltp:group_contest_eligibility AS g On c.contest_eligibility_id = g.contest_eligibility_id
                where c.is_studio = ? and c.contest_id = ?
                    """;
        final Long contestId = Helper.extract(request::hasContestId, request::getContestId);
        final Boolean studio = Helper.extract(request::hasStudio, request::getStudio);
        List<GroupContestEligibilityProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GroupContestEligibilityProto.Builder builder = GroupContestEligibilityProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setContestEligibilityId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setContestId);
            ResultSetHelper.applyResultSetBool(rs, 3, builder::setStudio);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setGroupId);
            return builder.build();
        }, studio, contestId);
        responseObserver
                .onNext(ContestEligibilitiesResponse.newBuilder().addAllGroupContestEligibilities(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void haveEligibility(HaveEligibilityRequest request,
            StreamObserver<HaveEligibilityResponse> responseObserver) {
        validateHaveEligibilityRequest(request);
        String sql = """
                select unique contest_id
                from common_oltp:contest_eligibility
                where is_studio = ? and contest_id in (%s)
                """;
        final Boolean studio = Helper.extract(request::hasStudio, request::getStudio);
        int startIndex = 0;
        List<Long> result = new ArrayList<>();
        while (true) {
            int count = Math.min(100, request.getContestIdsCount() - startIndex);
            String inSql = Helper.getInClause(count);
            List<Object> param = new ArrayList<>();
            param.add(studio);
            param.addAll(request.getContestIdsList().stream().skip(startIndex).limit(count).toList());
            List<Long> qResult = dbAccessor.executeQuery(sql.formatted(inSql),
                    (rs, _i) -> {
                        return Long.valueOf(rs.getLong(1));
                    }, param.toArray());
            result.addAll(qResult);
            startIndex += count;
            if (startIndex >= request.getContestIdsCount()) {
                break;
            }
        }
        responseObserver
                .onNext(HaveEligibilityResponse.newBuilder().addAllContestIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void validateUserContestEligibility(ValidateUserContestEligibilityRequest request,
            StreamObserver<ValidateUserContestEligibilityResponse> responseObserver) {
        validateValidateUserContestEligibilityRequest(request);
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM user_group_xref WHERE security_status_id = 1 and login_id = ? and group_id = ?) THEN 1 ELSE 0 END
                """;
        final Long userId = Helper.extract(request::hasUserId, request::getUserId);
        final Long groupId = Helper.extract(request::hasGroupId, request::getGroupId);
        boolean result = dbAccessor.executeQuery(dbAccessor.getPgJdbcTemplate(), sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, userId, groupId).get(0);

        ValidateUserContestEligibilityResponse response = ValidateUserContestEligibilityResponse.newBuilder()
                .setIsValid(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void validateCreateRequest(CreateRequest request) {
        Helper.assertObjectNotNull(request::hasContestId, "contestId");
        Helper.assertObjectNotNull(request::hasStudio, "studio");
    }

    private void validateRemoveRequest(RemoveRequest request) {
        Helper.assertObjectNotNull(request::hasContestEligibilityId, "contestEligibilityId");
    }

    private void validateBulkRemoveRequest(BulkRemoveRequest request) {
        Helper.assertObjectNotEmpty(request::getContestEligibilityIdsCount, "contestEligibilityId");
    }

    private void validateUpdateRequest(UpdateRequest request) {
        Helper.assertObjectNotNull(request::hasContestId, "contestId");
        Helper.assertObjectNotNull(request::hasStudio, "studio");
        Helper.assertObjectNotNull(() -> request.hasContestEligibilityId(), "contestEligibilityId");
    }

    private void validateGetContestEligibilityRequest(GetContestEligibilityRequest request) {
        Helper.assertObjectNotNull(request::hasContestId, "contestId");
        Helper.assertObjectNotNull(request::hasStudio, "studio");
    }

    private void validateHaveEligibilityRequest(HaveEligibilityRequest request) {
        Helper.assertObjectNotNull(request::hasStudio, "studio");
        Helper.assertObjectNotEmpty(request::getContestIdsCount, "contestId");
    }

    private void validateValidateUserContestEligibilityRequest(ValidateUserContestEligibilityRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "userId");
        Helper.assertObjectNotNull(request::hasGroupId, "groupId");
    }
}