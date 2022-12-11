package com.topcoder.or.repository;

import com.google.protobuf.Empty;
import com.google.protobuf.Int64Value;
import com.topcoder.onlinereview.grpc.contesteligibility.proto.*;
import com.topcoder.or.util.DBAccessor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;

import java.util.List;

@GrpcService
public class ContestEligibilityService extends ContestEligibilityServiceGrpc.ContestEligibilityServiceImplBase {
    private final DBAccessor dbAccessor;
    private final Logger logger;

    public ContestEligibilityService(DBAccessor dbAccessor, Logger logger) {
        this.dbAccessor = dbAccessor;
        this.logger = logger;
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
        Validation validation = validateCreateRequest(request);
        if (!validation.isValid()) {
            responseObserver
                    .onError(Status.INVALID_ARGUMENT.withDescription(validation.getMessage()).asRuntimeException());
            return;
        }
        try {
            final long contestId = request.getContestId().getValue();
            final int isStudio = request.getStudio().getValue() ? 1 : 0;

            int inserted = dbAccessor.executeUpdate(
                    "insert into contest_eligibility(contest_eligibility_id, contest_id, is_studio) VALUES(CONTEST_ELIGIBILITY_SEQ.NEXTVAL, ?, ?)",
                    contestId,
                    isStudio);
            if (inserted != 1) {
                responseObserver.onError(Status.INTERNAL.withDescription("Insert failed").asRuntimeException());
                return;
            }
            List<Int64Value> result = dbAccessor.executeQuery(
                    "select max(contest_eligibility_id) from contest_eligibility where contest_id = ?", (rs, _i) -> {
                        return Int64Value.of(rs.getLong(1));
                    }, contestId);
            CreateResponse.Builder rBuilder = CreateResponse.newBuilder();
            if (!result.isEmpty()) {
                rBuilder.setContestEligibilityId(result.get(0));
            }
            responseObserver.onNext(rBuilder.build());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<Empty> responseObserver) {
        super.remove(request, responseObserver);
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

    private Validation validateCreateRequest(CreateRequest createRequest) {
        Validation validation = new Validation();
        if (!createRequest.hasContestId()) {
            validation.setRequiredFieldMessage("contestId");
        } else if (!createRequest.hasStudio()) {
            validation.setRequiredFieldMessage("studio");
        }
        return validation;
    }
}