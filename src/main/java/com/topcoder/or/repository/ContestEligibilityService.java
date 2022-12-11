package com.topcoder.or.repository;

import com.google.protobuf.Empty;
import com.google.protobuf.Int64Value;
import com.topcoder.onlinereview.grpc.contesteligibility.proto.*;
import com.topcoder.or.util.DBAccessor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

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

        int inserted = dbAccessor.executeUpdate(
                "insert into common_oltp:contest_eligibility(contest_eligibility_id, contest_id, is_studio) VALUES(common_oltp:CONTEST_ELIGIBILITY_SEQ.NEXTVAL, ?, ?)",
                contestId,
                isStudio);
        if (inserted != 1) {
            responseObserver.onError(Status.INTERNAL.withDescription("Insert failed").asRuntimeException());
            return;
        }
        List<Int64Value> result = dbAccessor.executeQuery(
                "select max(contest_eligibility_id) from common_oltp:contest_eligibility where contest_id = ?",
                (rs, _i) -> {
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
        final long contestEligibilityId = request.getContestEligibilityId().getValue();
        dbAccessor.executeUpdate("delete from common_oltp:contest_eligibility where contest_eligibility_id = ?",
                contestEligibilityId);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void bulkRemove(BulkRemoveRequest request, StreamObserver<Empty> responseObserver) {
        super.bulkRemove(request, responseObserver);
    }

    @Override
    public void update(UpdateRequest request, StreamObserver<Empty> responseObserver) {
        super.update(request, responseObserver);
    }

    @Override
    public void getContestEligibility(GetContestEligibilityRequest request,
            StreamObserver<ContestEligibilitiesResponse> responseObserver) {
        super.getContestEligibility(request, responseObserver);
    }

    @Override
    public void haveEligibility(HaveEligibilityRequest request,
            StreamObserver<HaveEligibilityResponse> responseObserver) {
        super.haveEligibility(request, responseObserver);
    }

    private void validateCreateRequest(CreateRequest createRequest) {
        if (!createRequest.hasContestId()) {
            throw new IllegalArgumentException("contestId is required");
        } else if (!createRequest.hasStudio()) {
            throw new IllegalArgumentException("studio is required");
        }
    }

    private void validateRemoveRequest(RemoveRequest removeRequest) {
        if (!removeRequest.hasContestEligibilityId()) {
            throw new IllegalArgumentException("contestEligibilityId is required");
        }
    }
}