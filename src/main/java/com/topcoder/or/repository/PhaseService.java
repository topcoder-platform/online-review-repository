package com.topcoder.or.repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.topcoder.onlinereview.component.id.DBHelper;
import com.topcoder.onlinereview.component.id.IDGenerator;
import com.topcoder.onlinereview.grpc.phase.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class PhaseService extends PhaseServiceGrpc.PhaseServiceImplBase {
    private final DBAccessor dbAccessor;
    private final DBHelper dbHelper;

    private IDGenerator idGenerator;

    private String idSeqName = "project_phase_id_seq";

    private static final int AUDIT_CREATE_TYPE = 1;
    private static final int AUDIT_UPDATE_TYPE = 3;

    public PhaseService(DBAccessor dbAccessor, DBHelper dbHelper) {
        this.dbAccessor = dbAccessor;
        this.dbHelper = dbHelper;
    }

    @PostConstruct
    public void postRun() {
        idGenerator = new IDGenerator(idSeqName, dbHelper);
    }

    @Override
    public void getProjectIds(ProjectIdsProto request, StreamObserver<ProjectIdsProto> responseObserver) {
        validateProjectIdsProto(request);
        String sql = """
                SELECT project_id FROM project WHERE project_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getProjectIdsCount());
        List<Long> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return rs.getLong(1);
        }, request.getProjectIdsList().toArray());
        responseObserver.onNext(ProjectIdsProto.newBuilder().addAllProjectIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPhasesByProjectIds(ProjectIdsProto request, StreamObserver<GetPhasesResponse> responseObserver) {
        validateProjectIdsProto(request);
        String sql = """
                SELECT project_phase_id, project_id, fixed_start_time, scheduled_start_time, scheduled_end_time, actual_start_time,
                actual_end_time, duration, project_phase.modify_user, project_phase.modify_date, phase_type_lu.phase_type_id,
                phase_type_lu.name phase_type_name, phase_status_lu.phase_status_id, phase_status_lu.name phase_status_name
                FROM project_phase
                JOIN phase_type_lu ON phase_type_lu.phase_type_id = project_phase.phase_type_id
                JOIN phase_status_lu ON phase_status_lu.phase_status_id = project_phase.phase_status_id
                WHERE project_id IN
                """;
        String inSql = " (%s)".formatted(Helper.getInClause(request.getProjectIdsCount()));
        List<PhaseProto> result = dbAccessor.executeQuery(sql.concat(inSql), (rs, _i) -> {
            PhaseProto.Builder builder = PhaseProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setProjectId);
            ResultSetHelper.applyResultSetTimestamp(rs, 3, builder::setFixedStartTime);
            ResultSetHelper.applyResultSetTimestamp(rs, 4, builder::setScheduledStartTime);
            ResultSetHelper.applyResultSetTimestamp(rs, 5, builder::setScheduledEndTime);
            ResultSetHelper.applyResultSetTimestamp(rs, 6, builder::setActualStartTime);
            ResultSetHelper.applyResultSetTimestamp(rs, 7, builder::setActualEndTime);
            ResultSetHelper.applyResultSetLong(rs, 8, builder::setDuration);
            ResultSetHelper.applyResultSetString(rs, 9, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 10, builder::setModifyDate);
            ResultSetHelper.applyResultSetLong(rs, 11, builder::setPhaseTypeId);
            ResultSetHelper.applyResultSetString(rs, 12, builder::setPhaseTypeName);
            ResultSetHelper.applyResultSetLong(rs, 13, builder::setPhaseStatusId);
            ResultSetHelper.applyResultSetString(rs, 14, builder::setPhaseStatusName);
            return builder.build();
        }, request.getProjectIdsList().toArray());
        responseObserver.onNext(GetPhasesResponse.newBuilder().addAllPhases(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectIdsByPhaseIds(PhaseIdsProto request,
            StreamObserver<GetProjectIdsByPhaseIdsResponse> responseObserver) {
        validatePhaseIdsProto(request);
        String sql = """
                SELECT project_phase_id, project_id FROM project_phase WHERE project_phase_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getPhaseIdsCount());
        List<PhaseIdProjectIdProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            PhaseIdProjectIdProto.Builder builder = PhaseIdProjectIdProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setProjectId);
            return builder.build();
        }, request.getPhaseIdsList().toArray());
        responseObserver.onNext(GetProjectIdsByPhaseIdsResponse.newBuilder().addAllPhaseIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPhaseCriteriasByProjectIds(ProjectIdsProto request,
            StreamObserver<GetPhaseCriteriasResponse> responseObserver) {
        validateProjectIdsProto(request);
        String sql = """
                SELECT phase_criteria.project_phase_id, phase_criteria.phase_criteria_type_id, name, parameter
                FROM phase_criteria
                JOIN phase_criteria_type_lu ON phase_criteria_type_lu.phase_criteria_type_id = phase_criteria.phase_criteria_type_id
                JOIN project_phase ON phase_criteria.project_phase_id = project_phase.project_phase_id
                WHERE project_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getProjectIdsCount());
        List<PhaseCriteriaProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            PhaseCriteriaProto.Builder builder = PhaseCriteriaProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setPhaseCriteriaTypeId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setCriteriaName);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setParameter);
            return builder.build();
        }, request.getProjectIdsList().toArray());
        responseObserver.onNext(GetPhaseCriteriasResponse.newBuilder().addAllPhaseCriteria(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPhaseCriteriasByPhaseId(PhaseIdProto request,
            StreamObserver<GetPhaseCriteriasResponse> responseObserver) {
        validatePhaseIdProto(request);
        String sql = """
                SELECT phase_criteria.phase_criteria_type_id, name, parameter
                FROM phase_criteria
                JOIN phase_criteria_type_lu ON phase_criteria_type_lu.phase_criteria_type_id = phase_criteria.phase_criteria_type_id
                WHERE project_phase_id = ?
                """;
        List<PhaseCriteriaProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            PhaseCriteriaProto.Builder builder = PhaseCriteriaProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setPhaseCriteriaTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setCriteriaName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setParameter);
            return builder.build();
        }, request.getPhaseId());
        responseObserver.onNext(GetPhaseCriteriasResponse.newBuilder().addAllPhaseCriteria(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPhaseDependenciesByProjectId(ProjectIdsProto request,
            StreamObserver<GetPhaseDependenciesResponse> responseObserver) {
        validateProjectIdsProto(request);
        String sql = """
                SELECT dependency_phase_id, dependent_phase_id, dependency_start, dependent_start, lag_time
                FROM phase_dependency
                JOIN project_phase ON dependent_phase_id = project_phase_id
                WHERE project_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getProjectIdsCount());
        List<PhaseDependencyProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            PhaseDependencyProto.Builder builder = PhaseDependencyProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setDependencyPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setDependentPhaseId);
            ResultSetHelper.applyResultSetBool(rs, 3, builder::setDependencyStart);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setDependentStart);
            ResultSetHelper.applyResultSetLong(rs, 5, builder::setLagTime);
            return builder.build();
        }, request.getProjectIdsList().toArray());
        responseObserver.onNext(GetPhaseDependenciesResponse.newBuilder().addAllPhaseDependencies(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPhaseDependenciesByDependentPhaseId(PhaseIdProto request,
            StreamObserver<GetPhaseDependenciesResponse> responseObserver) {
        validatePhaseIdProto(request);
        String sql = """
                SELECT dependency_phase_id, dependent_phase_id, dependency_start, dependent_start, lag_time
                FROM phase_dependency
                WHERE dependent_phase_id = ?
                """;
        List<PhaseDependencyProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            PhaseDependencyProto.Builder builder = PhaseDependencyProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setDependencyPhaseId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setDependentPhaseId);
            ResultSetHelper.applyResultSetBool(rs, 3, builder::setDependencyStart);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setDependentStart);
            ResultSetHelper.applyResultSetLong(rs, 5, builder::setLagTime);
            return builder.build();
        }, request.getPhaseId());
        responseObserver.onNext(GetPhaseDependenciesResponse.newBuilder().addAllPhaseDependencies(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllPhaseTypes(Empty request, StreamObserver<GetPhaseTypesResponse> responseObserver) {
        String sql = """
                SELECT phase_type_id, name phase_type_name FROM phase_type_lu
                """;
        List<PhaseTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            PhaseTypeProto.Builder builder = PhaseTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setPhaseTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setPhaseTypeName);
            return builder.build();
        });
        responseObserver.onNext(GetPhaseTypesResponse.newBuilder().addAllPhaseTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllPhaseStatuses(Empty request, StreamObserver<GetPhaseStatusesResponse> responseObserver) {
        String sql = """
                SELECT phase_status_id, name phase_status_name FROM phase_status_lu
                """;
        List<PhaseStatusProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            PhaseStatusProto.Builder builder = PhaseStatusProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setPhaseStatusId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setPhaseStatusName);
            return builder.build();
        });
        responseObserver.onNext(GetPhaseStatusesResponse.newBuilder().addAllPhaseStatuses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllPhaseCriteriaTypes(Empty request,
            StreamObserver<GetPhaseCriteriaTypesResponse> responseObserver) {
        String sql = """
                SELECT phase_criteria_type_id, name FROM phase_criteria_type_lu
                """;
        List<PhaseCriteriaTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            PhaseCriteriaTypeProto.Builder builder = PhaseCriteriaTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setPhaseCriteriaTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            return builder.build();
        });
        responseObserver.onNext(GetPhaseCriteriaTypesResponse.newBuilder().addAllPhaseCriteriaTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createPhase(CreatePhaseRequest request, StreamObserver<PhaseProto> responseObserver) {
        validateCreatePhaseRequest(request);
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        Long newId = idGenerator.getNextID();
        String sql = """
                INSERT INTO project_phase (project_phase_id, project_id, phase_type_id, phase_status_id, fixed_start_time, scheduled_start_time,
                scheduled_end_time, actual_start_time, actual_end_time, duration, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final PhaseProto phase = request.getPhase();
        final Date fixedStartTime = Helper.extractDate(phase::hasFixedStartTime, phase::getFixedStartTime);
        final Date actualStartTime = Helper.extractDate(phase::hasActualStartTime, phase::getActualStartTime);
        final Date actualEndTime = Helper.extractDate(phase::hasActualEndTime, phase::getActualEndTime);
        dbAccessor.executeUpdate(sql, newId, phase.getProjectId(), phase.getPhaseTypeId(), phase.getPhaseStatusId(),
                fixedStartTime, Helper.convertDate(phase.getScheduledStartTime()),
                Helper.convertDate(phase.getScheduledEndTime()), actualStartTime,
                actualEndTime, phase.getDuration(), request.getOperator(), now, request.getOperator(), now);
        auditProjectPhase(newId, AUDIT_CREATE_TYPE, Helper.convertDate(phase.getScheduledStartTime()),
                Helper.convertDate(phase.getScheduledEndTime()), Long.parseLong(request.getOperator()), now);
        responseObserver.onNext(PhaseProto.newBuilder(phase).setProjectPhaseId(newId)
                .setModifyUser(request.getOperator()).setModifyDate(nowTs).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createPhaseCriteria(CreatePhaseCriteriaRequest request, StreamObserver<CountProto> responseObserver) {
        validateCreatePhaseCriteriaRequest(request);
        String sql = """
                INSERT INTO phase_criteria(project_phase_id, phase_criteria_type_id, parameter, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, CURRENT, ?, CURRENT)
                """;
        final PhaseCriteriaProto phase = request.getPhaseCriteria();
        int affected = dbAccessor.executeUpdate(sql, phase.getProjectPhaseId(), phase.getPhaseCriteriaTypeId(),
                phase.getParameter(), request.getOperator(), request.getOperator());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createPhaseDependency(CreatePhaseDependencyRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateCreatePhaseDependencyRequest(request);
        String sql = """
                INSERT INTO phase_dependency (dependency_phase_id, dependent_phase_id, dependency_start, dependent_start, lag_time,
                create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT, ?, CURRENT)
                """;
        final PhaseDependencyProto phaseDependency = request.getPhaseDependency();
        int affected = dbAccessor.executeUpdate(sql, phaseDependency.getDependencyPhaseId(),
                phaseDependency.getDependentPhaseId(), phaseDependency.getDependencyStart(),
                phaseDependency.getDependentStart(), phaseDependency.getLagTime(), request.getOperator(),
                request.getOperator());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePhase(UpdatePhaseRequest request, StreamObserver<UpdatePhaseResponse> responseObserver) {
        validateUpdatePhaseRequest(request);
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        Map<Long, PhaseProto> oldPhases = getPhaseMapForUpdateAudit(
                request.getPhasesList().stream().map(PhaseProto::getProjectPhaseId).toList());
        for (PhaseProto phase : request.getPhasesList()) {
            updatePhase(phase, request.getOperator(), now);
            PhaseProto oldPhase = oldPhases.get(phase.getProjectPhaseId());
            if (oldPhase.getScheduledStartTime().getSeconds() != phase.getScheduledStartTime().getSeconds()
                    || oldPhase.getScheduledEndTime().getSeconds() != phase.getScheduledEndTime().getSeconds()) {
                auditProjectPhase(phase.getProjectPhaseId(), AUDIT_UPDATE_TYPE,
                        Helper.convertDate(phase.getScheduledStartTime()),
                        Helper.convertDate(phase.getScheduledEndTime()), Long.parseLong(request.getOperator()), now);
            }
        }
        responseObserver
                .onNext(UpdatePhaseResponse.newBuilder().setModifyDate(nowTs).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePhaseCriteria(UpdatePhaseCriteriaRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdatePhaseCriteriaRequest(request);
        String sql = """
                UPDATE phase_criteria SET parameter = ?, modify_user = ?, modify_date = CURRENT
                WHERE project_phase_id = ? AND phase_criteria_type_id = ?
                """;
        final PhaseCriteriaProto phase = request.getPhaseCriteria();
        int affected = dbAccessor.executeUpdate(sql, phase.getParameter(), request.getOperator(),
                phase.getProjectPhaseId(), phase.getPhaseCriteriaTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePhaseDependency(UpdatePhaseDependencyRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateUpdatePhaseDependencyRequest(request);
        String sql = """
                UPDATE phase_dependency SET dependency_start = ?, dependent_start = ?, lag_time = ?, modify_user = ?, modify_date = CURRENT
                WHERE dependency_phase_id = ? AND dependent_phase_id = ?
                """;
        final PhaseDependencyProto phaseDependency = request.getPhaseDependency();
        int affected = dbAccessor.executeUpdate(sql, phaseDependency.getDependencyStart(),
                phaseDependency.getDependentStart(), phaseDependency.getLagTime(), request.getOperator(),
                phaseDependency.getDependencyPhaseId(), phaseDependency.getDependentPhaseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePhases(PhaseIdsProto request, StreamObserver<CountProto> responseObserver) {
        validatePhaseIdsProto(request);
        deletePhaseDependency(request.getPhaseIdsList());
        deletePhaseCriteria(request.getPhaseIdsList());
        deletePhaseAudit(request.getPhaseIdsList());
        int affected = deletePhase(request.getPhaseIdsList());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePhaseDependencies(DeletePhaseDependencyRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeletePhaseDependencyRequest(request);
        String sql = """
                DELETE FROM phase_dependency WHERE dependent_phase_id = ? AND dependency_phase_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getDependencyPhaseIdsCount());
        List<Object> param = new ArrayList<>();
        param.add(request.getDependentPhaseId());
        param.addAll(request.getDependencyPhaseIdsList());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), param.toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePhaseCriterias(DeletePhaseCriteriasRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeletePhaseCriteriasRequest(request);
        String sql = """
                DELETE FROM phase_criteria WHERE project_phase_id = ? AND phase_criteria_type_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getPhaseCriteriaTypeIdsCount());
        List<Object> param = new ArrayList<>();
        param.add(request.getProjectPhaseId());
        param.addAll(request.getPhaseCriteriaTypeIdsList());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), param.toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void checkIfDependencyExists(DependencyExistenceRequest request,
            StreamObserver<DependencyExistenceResponse> responseObserver) {
        validateDependencyExistenceRequest(request);
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM phase_dependency WHERE dependency_phase_id = ? AND dependent_phase_id = ?)
                THEN 1 ELSE 0 END FROM DUAL
                """;
        boolean exists = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, request.getDependencyPhaseId(), request.getDependentPhaseId()).get(0);
        responseObserver.onNext(DependencyExistenceResponse.newBuilder().setExists(exists).build());
        responseObserver.onCompleted();
    }

    private Map<Long, PhaseProto> getPhaseMapForUpdateAudit(List<Long> phaseIds) {
        String sql = """
                SELECT project_phase_id, scheduled_start_time, scheduled_end_time
                FROM project_phase
                WHERE project_phase_id IN (%s)
                """;
        String inSql = Helper.getInClause(phaseIds.size());
        List<PhaseProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            PhaseProto.Builder builder = PhaseProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectPhaseId);
            ResultSetHelper.applyResultSetTimestamp(rs, 2, builder::setScheduledStartTime);
            ResultSetHelper.applyResultSetTimestamp(rs, 3, builder::setScheduledEndTime);
            return builder.build();
        }, phaseIds.toArray());
        return result.stream().collect(Collectors.toMap(p -> p.getProjectPhaseId(), p -> p));
    }

    private void updatePhase(PhaseProto phase, String operator, Date modifyDate) {
        String sql = """
                UPDATE project_phase SET project_id = ?, phase_type_id = ?, phase_status_id = ?, fixed_start_time = ?, scheduled_start_time = ?,
                scheduled_end_time = ?, actual_start_time = ?, actual_end_time = ?, duration = ?, modify_user = ?, modify_date = ?
                WHERE project_phase_id = ?
                """;
        final Date fixedStartTime = Helper.extractDate(phase::hasFixedStartTime, phase::getFixedStartTime);
        final Date actualStartTime = Helper.extractDate(phase::hasActualStartTime, phase::getActualStartTime);
        final Date actualEndTime = Helper.extractDate(phase::hasActualEndTime, phase::getActualEndTime);
        dbAccessor.executeUpdate(sql, phase.getProjectId(), phase.getPhaseTypeId(), phase.getPhaseStatusId(),
                fixedStartTime, Helper.convertDate(phase.getScheduledStartTime()),
                Helper.convertDate(phase.getScheduledEndTime()), actualStartTime, actualEndTime, phase.getDuration(),
                operator, modifyDate, phase.getProjectPhaseId());
    }

    private void auditProjectPhase(long phaseId, int auditType, Date scheduledStartTime, Date scheduledEndTime,
            Long auditUser, Date auditTime) {
        String sql = """
                INSERT INTO project_phase_audit (project_phase_id, scheduled_start_time, scheduled_end_time, audit_action_type_id, action_date, action_user_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, phaseId, scheduledStartTime, scheduledEndTime, auditType, auditTime, auditUser);
    }

    private int deletePhaseDependency(List<Long> phaseIds) {
        if (phaseIds.isEmpty()) {
            return 0;
        }
        String sql = """
                DELETE FROM phase_dependency WHERE dependency_phase_id IN (%s) OR dependent_phase_id IN (%s)
                """;
        String inSql = Helper.getInClause(phaseIds.size());
        phaseIds.addAll(phaseIds);
        return dbAccessor.executeUpdate(sql.formatted(inSql, inSql), phaseIds.toArray());
    }

    private int deletePhaseCriteria(List<Long> phaseIds) {
        if (phaseIds.isEmpty()) {
            return 0;
        }
        String sql = """
                DELETE FROM phase_criteria WHERE project_phase_id IN (%s)
                """;
        String inSql = Helper.getInClause(phaseIds.size());
        return dbAccessor.executeUpdate(sql.formatted(inSql), phaseIds.toArray());
    }

    private int deletePhaseAudit(List<Long> phaseIds) {
        if (phaseIds.isEmpty()) {
            return 0;
        }
        String sql = """
                DELETE FROM project_phase_audit WHERE project_phase_id IN (%s)
                """;
        String inSql = Helper.getInClause(phaseIds.size());
        return dbAccessor.executeUpdate(sql.formatted(inSql), phaseIds.toArray());
    }

    private int deletePhase(List<Long> phaseIds) {
        if (phaseIds.isEmpty()) {
            return 0;
        }
        String sql = """
                DELETE FROM project_phase WHERE project_phase_id IN (%s)
                """;
        String inSql = Helper.getInClause(phaseIds.size());
        return dbAccessor.executeUpdate(sql.formatted(inSql), phaseIds.toArray());
    }

    private void validateProjectIdsProto(ProjectIdsProto request) {
        Helper.assertObjectNotEmpty(request::getProjectIdsCount, "project_ids");
    }

    private void validatePhaseIdsProto(PhaseIdsProto request) {
        Helper.assertObjectNotEmpty(request::getPhaseIdsCount, "phase_ids");
    }

    private void validatePhaseIdProto(PhaseIdProto request) {
        Helper.assertObjectNotNull(request::hasPhaseId, "phase_id");
    }

    private void validateCreatePhaseRequest(CreatePhaseRequest request) {
        Helper.assertObjectNotNull(request::hasPhase, "phase");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        PhaseProto phase = request.getPhase();
        Helper.assertObjectNotNull(phase::hasProjectId, "project_id");
        Helper.assertObjectNotNull(phase::hasPhaseTypeId, "phase_type_id");
        Helper.assertObjectNotNull(phase::hasPhaseStatusId, "phase_status_id");
        Helper.assertObjectNotNull(phase::hasScheduledStartTime, "scheduled_start_time");
        Helper.assertObjectNotNull(phase::hasScheduledEndTime, "scheduled_end_time");
        Helper.assertObjectNotNull(phase::hasDuration, "duration");
    }

    private void validateCreatePhaseCriteriaRequest(CreatePhaseCriteriaRequest request) {
        Helper.assertObjectNotNull(request::hasPhaseCriteria, "phase_criteria");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        PhaseCriteriaProto phaseCriteria = request.getPhaseCriteria();
        Helper.assertObjectNotNull(phaseCriteria::hasProjectPhaseId, "project_phase_id");
        Helper.assertObjectNotNull(phaseCriteria::hasPhaseCriteriaTypeId, "phase_criteria_type_id");
        Helper.assertObjectNotNull(phaseCriteria::hasParameter, "parameter");
    }

    private void validateCreatePhaseDependencyRequest(CreatePhaseDependencyRequest request) {
        Helper.assertObjectNotNull(request::hasPhaseDependency, "phase_dependency");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        PhaseDependencyProto phaseDependency = request.getPhaseDependency();
        Helper.assertObjectNotNull(phaseDependency::hasDependencyPhaseId, "dependency_phase_id");
        Helper.assertObjectNotNull(phaseDependency::hasDependentPhaseId, "dependent_phase_id");
        Helper.assertObjectNotNull(phaseDependency::hasDependencyStart, "dependency_start");
        Helper.assertObjectNotNull(phaseDependency::hasDependentStart, "dependent_start");
        Helper.assertObjectNotNull(phaseDependency::hasLagTime, "lag_time");
    }

    private void validateUpdatePhaseRequest(UpdatePhaseRequest request) {
        Helper.assertObjectNotEmpty(request::getPhasesCount, "phases");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        for (PhaseProto phase : request.getPhasesList()) {
            Helper.assertObjectNotNull(phase::hasProjectPhaseId, "project_phase_id");
            Helper.assertObjectNotNull(phase::hasProjectId, "project_id");
            Helper.assertObjectNotNull(phase::hasPhaseTypeId, "phase_type_id");
            Helper.assertObjectNotNull(phase::hasPhaseStatusId, "phase_status_id");
            Helper.assertObjectNotNull(phase::hasScheduledStartTime, "scheduled_start_time");
            Helper.assertObjectNotNull(phase::hasScheduledEndTime, "scheduled_end_time");
            Helper.assertObjectNotNull(phase::hasDuration, "duration");
        }
    }

    private void validateUpdatePhaseCriteriaRequest(UpdatePhaseCriteriaRequest request) {
        Helper.assertObjectNotNull(request::hasPhaseCriteria, "phase_criteria");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        PhaseCriteriaProto phaseCriteria = request.getPhaseCriteria();
        Helper.assertObjectNotNull(phaseCriteria::hasProjectPhaseId, "project_phase_id");
        Helper.assertObjectNotNull(phaseCriteria::hasPhaseCriteriaTypeId, "phase_criteria_type_id");
        Helper.assertObjectNotNull(phaseCriteria::hasParameter, "parameter");
    }

    private void validateUpdatePhaseDependencyRequest(UpdatePhaseDependencyRequest request) {
        Helper.assertObjectNotNull(request::hasPhaseDependency, "phase_dependency");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        PhaseDependencyProto phaseDependency = request.getPhaseDependency();
        Helper.assertObjectNotNull(phaseDependency::hasDependencyPhaseId, "dependency_phase_id");
        Helper.assertObjectNotNull(phaseDependency::hasDependentPhaseId, "dependent_phase_id");
        Helper.assertObjectNotNull(phaseDependency::hasDependencyStart, "dependency_start");
        Helper.assertObjectNotNull(phaseDependency::hasDependentStart, "dependent_start");
        Helper.assertObjectNotNull(phaseDependency::hasLagTime, "lag_time");
    }

    private void validateDeletePhaseDependencyRequest(DeletePhaseDependencyRequest request) {
        Helper.assertObjectNotNull(request::hasDependentPhaseId, "dependent_phase_id");
        Helper.assertObjectNotEmpty(request::getDependencyPhaseIdsCount, "dependency_phase_ids");
    }

    private void validateDeletePhaseCriteriasRequest(DeletePhaseCriteriasRequest request) {
        Helper.assertObjectNotNull(request::hasProjectPhaseId, "project_phase_id");
        Helper.assertObjectNotEmpty(request::getPhaseCriteriaTypeIdsCount, "phase_criteria_type_ids");
    }

    private void validateDependencyExistenceRequest(DependencyExistenceRequest request) {
        Helper.assertObjectNotNull(request::hasDependentPhaseId, "dependent_phase_id");
        Helper.assertObjectNotNull(request::hasDependencyPhaseId, "dependency_phase_id");
    }
}
