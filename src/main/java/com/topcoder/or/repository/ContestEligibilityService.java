package com.topcoder.or.repository;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.Int64Value;
import com.topcoder.onlinereview.grpc.contesteligibility.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.ArrayList;
import java.util.Collections;
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
        final long contestId = request.getContestId().getValue();
        final int isStudio = request.getStudio().getValue() ? 1 : 0;
        String sql = """
                insert into common_oltp:contest_eligibility(contest_eligibility_id, contest_id, is_studio)
                VALUES(common_oltp:CONTEST_ELIGIBILITY_SEQ.NEXTVAL, ?, ?)
                    """;
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
        List<Int64Value> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return Int64Value.of(rs.getLong(1));
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
        final long contestEligibilityId = request.getContestEligibilityId().getValue();
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
        String inSql = String.join(",", Collections.nCopies(request.getContestEligibilityIdsCount(), "?"));
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
        dbAccessor.executeUpdate(sql, request.getContestId().getValue(), request.getStudio().getValue(),
                request.getContestEligibilityId().getValue());
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
        List<GroupContestEligibilityProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GroupContestEligibilityProto.Builder builder = GroupContestEligibilityProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, v -> {
                builder.setContestEligibilityId(Int64Value.of(v));
            });
            ResultSetHelper.applyResultSetLong(rs, 2, v -> {
                builder.setContestId(Int64Value.of(v));
            });
            ResultSetHelper.applyResultSetBool(rs, 3, v -> {
                builder.setStudio(BoolValue.of(v));
            });
            ResultSetHelper.applyResultSetLong(rs, 4, v -> {
                builder.setGroupId(Int64Value.of(v));
            });
            return builder.build();
        }, request.getStudio().getValue() ? 1 : 0, request.getContestId().getValue());
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
                where is_studio = %d and contest_id in (%s)
                """;
        final int isStudio = request.getStudio().getValue() ? 1 : 0;
        int startIndex = 0;
        List<Long> result = new ArrayList<>();
        while (true) {
            int count = Math.min(100, request.getContestIdsCount() - startIndex);
            String inSql = String.join(",", Collections.nCopies(count, "?"));
            List<Long> qResult = dbAccessor.executeQuery(sql.formatted(isStudio, inSql),
                    (rs, _i) -> {
                        return Long.valueOf(rs.getLong(1));
                    }, request.getContestIdsList().toArray());
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
                select first 1 user_group_id
                from common_oltp:user_group_xref
                where security_status_id = 1 and login_id = ? and group_id = ?
                    """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getUserId().getValue(), request.getGroupId().getValue());
        ValidateUserContestEligibilityResponse response = ValidateUserContestEligibilityResponse.newBuilder()
                .setIsValid(!result.isEmpty()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void validateCreateRequest(CreateRequest request) {
        Helper.assertObjectNotNull(() -> request.hasContestId(), "contestId");
        Helper.assertObjectNotNull(() -> request.hasStudio(), "studio");
    }

    private void validateRemoveRequest(RemoveRequest request) {
        Helper.assertObjectNotNull(() -> request.hasContestEligibilityId(), "contestEligibilityId");
    }

    private void validateBulkRemoveRequest(BulkRemoveRequest request) {
        Helper.assertObjectNotNull(() -> request.getContestEligibilityIdsList().isEmpty(), "contestEligibilityId");
    }

    private void validateUpdateRequest(UpdateRequest request) {
        Helper.assertObjectNotNull(() -> request.hasContestId(), "contestId");
        Helper.assertObjectNotNull(() -> request.hasStudio(), "studio");
        Helper.assertObjectNotNull(() -> request.hasContestEligibilityId(), "contestEligibilityId");
    }

    private void validateGetContestEligibilityRequest(GetContestEligibilityRequest request) {
        Helper.assertObjectNotNull(() -> request.hasContestId(), "contestId");
        Helper.assertObjectNotNull(() -> request.hasStudio(), "studio");
    }

    private void validateHaveEligibilityRequest(HaveEligibilityRequest request) {
        Helper.assertObjectNotNull(() -> request.hasStudio(), "studio");
        Helper.assertObjectNotNull(() -> request.getContestIdsList().isEmpty(), "contestId");
    }

    private void validateValidateUserContestEligibilityRequest(ValidateUserContestEligibilityRequest request) {
        Helper.assertObjectNotNull(() -> request.hasUserId(), "userId");
        Helper.assertObjectNotNull(() -> request.hasGroupId(), "groupId");
    }
}