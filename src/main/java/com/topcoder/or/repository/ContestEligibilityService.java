package com.topcoder.or.repository;

import com.google.protobuf.Empty;
import com.google.protobuf.Int64Value;
import com.topcoder.onlinereview.contesteligibility.proto.*;
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
    public void create(CreateRequest request, StreamObserver<Empty> responseObserver) {
        try {
            final long contestId = request.getContestId().getValue();
            final int isStudio = request.getStudio().getValue() ? 1 : 0;

            this.dbAccessor
                    .executeUpdate("insert into contest_eligibility(contest_eligibility_id, contest_id, is_studio) VALUES(CONTEST_ELIGIBILITY_SEQ.NEXTVAL, ?, ?)",
                        contestId,
                        isStudio
                    );

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable e) {
            this.logger.error(e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getIdByContestId(GetIdByContestIdRequest request, StreamObserver<GetIdByContestIdResponse> responseObserver) {
        try {
            final long contestId = request.getContestId().getValue();

            final List<String[]> result = this.dbAccessor
                    .executeQuery("FROM contest_eligibility WHERE contest_id = ?", new String[]{String.valueOf(contestId)}, new String[]{"contest_eligibility_id"});

            if (result.size() > 0) {
                responseObserver.onNext(GetIdByContestIdResponse.newBuilder()
                        .setContestEligibilityId(Int64Value.of(Long.parseLong(result.get(0)[0])))
                        .build()
                );
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new Exception("No contest eligibility found for contest id " + contestId));
            }
        } catch (Throwable e) {
            this.logger.error(e.getMessage(), e);
            responseObserver.onError(e);
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
    public void getContestEligibility(GetContestEligibilityRequest request, StreamObserver<ContestEligibilitiesResponse> responseObserver) {
        super.getContestEligibility(request, responseObserver);
    }

    @Override
    public void haveEligibility(HaveEligibilityRequest request, StreamObserver<HaveEligibilityResponse> responseObserver) {
        super.haveEligibility(request, responseObserver);
    }
}