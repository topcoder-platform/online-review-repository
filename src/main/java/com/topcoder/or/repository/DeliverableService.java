package com.topcoder.or.repository;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.grpc.deliverable.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@GrpcService
public class DeliverableService extends DeliverableServiceGrpc.DeliverableServiceImplBase {
    private final DBAccessor dbAccessor;

    public DeliverableService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
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
            ResultSetHelper.applyResultSetLong(rs, 1, v -> {
                builder.setProjectId(v);
            });
            ResultSetHelper.applyResultSetLong(rs, 2, v -> {
                builder.setProjectPhaseId(v);
            });
            ResultSetHelper.applyResultSetLong(rs, 3, v -> {
                builder.setResourceId(v);
            });
            ResultSetHelper.applyResultSetBool(rs, 4, v -> {
                builder.setRequired(v);
            });
            ResultSetHelper.applyResultSetLong(rs, 5, v -> {
                builder.setDeliverableId(v);
            });
            ResultSetHelper.applyResultSetString(rs, 6, v -> {
                builder.setCreateUser(v);
            });
            ResultSetHelper.applyResultSetTimestamp(rs, 7, v -> {
                builder.setCreateDate(v);
            });
            ResultSetHelper.applyResultSetString(rs, 8, v -> {
                builder.setModifyUser(v);
            });
            ResultSetHelper.applyResultSetTimestamp(rs, 9, v -> {
                builder.setModifyDate(v);
            });
            ResultSetHelper.applyResultSetString(rs, 10, v -> {
                builder.setName(v);
            });
            ResultSetHelper.applyResultSetString(rs, 11, v -> {
                builder.setDescription(v);
            });
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
            ResultSetHelper.applyResultSetLong(rs, 1, v -> {
                builder.setProjectId(v);
            });
            ResultSetHelper.applyResultSetLong(rs, 2, v -> {
                builder.setProjectPhaseId(v);
            });
            ResultSetHelper.applyResultSetLong(rs, 3, v -> {
                builder.setResourceId(v);
            });
            ResultSetHelper.applyResultSetLong(rs, 4, v -> {
                builder.setSubmissionId(v);
            });
            ResultSetHelper.applyResultSetBool(rs, 5, v -> {
                builder.setRequired(v);
            });
            ResultSetHelper.applyResultSetLong(rs, 6, v -> {
                builder.setDeliverableId(v);
            });
            ResultSetHelper.applyResultSetString(rs, 7, v -> {
                builder.setCreateUser(v);
            });
            ResultSetHelper.applyResultSetTimestamp(rs, 8, v -> {
                builder.setCreateDate(v);
            });
            ResultSetHelper.applyResultSetString(rs, 9, v -> {
                builder.setModifyUser(v);
            });
            ResultSetHelper.applyResultSetTimestamp(rs, 10, v -> {
                builder.setModifyDate(v);
            });
            ResultSetHelper.applyResultSetString(rs, 11, v -> {
                builder.setName(v);
            });
            ResultSetHelper.applyResultSetString(rs, 12, v -> {
                builder.setDescription(v);
            });
            return builder.build();
        });
        responseObserver.onNext(LoadDeliverablesWithSubmissionResponse.newBuilder()
                .addAllDeliverablesWithSubmissions(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateLateDeliverable(UpdateLateDeliverableRequest request,
            StreamObserver<UpdatedCountProto> responseObserver) {
        super.updateLateDeliverable(request, responseObserver);
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
        Helper.assertObjectNotNull(() -> !request.getDeliverableIdsList().isEmpty(), "deliverable_ids");
        Helper.assertObjectNotNull(() -> !request.getPhaseIdsList().isEmpty(), "phase_ids");
        Helper.assertObjectNotNull(() -> !request.getResourceIdsList().isEmpty(), "resource_ids");
        if (request.getDeliverableIdsCount() != request.getResourceIdsCount()
                || request.getDeliverableIdsCount() != request.getPhaseIdsCount()) {
            throw new IllegalArgumentException(
                    "deliverableIds, resourceIds and phaseIds should have the same number of elements.");
        }
    }

    private void validateLoadDeliverablesWithSubmissionRequest(LoadDeliverablesWithSubmissionRequest request) {
        Helper.assertObjectNotNull(() -> !request.getDeliverableIdsList().isEmpty(), "deliverable_ids");
        Helper.assertObjectNotNull(() -> !request.getPhaseIdsList().isEmpty(), "phase_ids");
        Helper.assertObjectNotNull(() -> !request.getResourceIdsList().isEmpty(), "resource_ids");
        Helper.assertObjectNotNull(() -> !request.getSubmissionIdsList().isEmpty(), "submission_ids");
        if (request.getDeliverableIdsCount() != request.getResourceIdsCount()
                || request.getDeliverableIdsCount() != request.getPhaseIdsCount()
                || request.getDeliverableIdsCount() != request.getSubmissionIdsCount()) {
            throw new IllegalArgumentException(
                    "deliverableIds, resourceIds, phaseIds and submissionIds should have the same number of elements.");
        }
    }
}