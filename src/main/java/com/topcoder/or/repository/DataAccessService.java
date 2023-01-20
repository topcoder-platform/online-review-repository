package com.topcoder.or.repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.topcoder.onlinereview.component.idgenerator.IdGenerator;
import com.topcoder.onlinereview.component.shared.dataaccess.DataAccess;
import com.topcoder.onlinereview.component.shared.dataaccess.Request;
import com.topcoder.onlinereview.grpc.dataaccess.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class DataAccessService extends DataAccessServiceGrpc.DataAccessServiceImplBase {
    private final DBAccessor dbAccessor;

    public DataAccessService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void getComponentVersionInfo(GetComponentVersionInfoRequest request,
            StreamObserver<GetComponentVersionInfoResponse> responseObserver) {
        validateGetComponentVersionInfoRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "comp_version";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("cd", String.valueOf(request.getComponentId()));
        dbRequest.setProperty("vid", String.valueOf(request.getVersionNumber()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> versionData = result.get(queryName);
        GetComponentVersionInfoResponse response;
        if (versionData.isEmpty()) {
            response = GetComponentVersionInfoResponse.getDefaultInstance();
        } else {
            GetComponentVersionInfoResponse.Builder builder = GetComponentVersionInfoResponse.newBuilder();
            Long versionId = Helper.getLong(versionData.get(0), "version_id");
            if (versionId != null) {
                builder.setVersionId(versionId);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getDocuments(GetDocumentsRequest request, StreamObserver<GetDocumentsResponse> responseObserver) {
        validateGetDocumentsRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "comp_version_documents";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("cv", String.valueOf(request.getComponentVersionId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<DocumentProto> documents = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            DocumentProto.Builder document = DocumentProto.newBuilder();
            Long documentId = Helper.getLong(data.get(i), "document_id");
            if (documentId != null) {
                document.setDocumentId(documentId);
            }
            String documentName = Helper.getString(data.get(i), "document_name");
            if (documentName != null) {
                document.setDocumentName(documentName);
            }
            String url = Helper.getString(data.get(i), "url");
            if (url != null) {
                document.setUrl(url);
            }
            Long documentTypeId = Helper.getLong(data.get(i), "document_type_id");
            if (documentTypeId != null) {
                document.setDocumentTypeId(documentTypeId);
            }
            documents.add(document.build());
        }
        responseObserver.onNext(GetDocumentsResponse.newBuilder().addAllDocuments(documents).build());
        responseObserver.onCompleted();
    }

    @Override
    public void addDocument(AddDocumentRequest request, StreamObserver<AddDocumentResponse> responseObserver) {
        validateAddDocumentRequest(request);
        long newId = generateNextCatalogScopedId();
        String sql = """
                INSERT INTO comp_documentation (document_id, comp_vers_id, document_type_id, document_name, url)
                VALUES (?, ?, ?, ?, ?)
                """;
        final Long componentVersionId = Helper.extract(request::hasComponentVersionId, request::getComponentVersionId);
        final Long documentTypeId = Helper.extract(request::hasDocumentTypeId, request::getDocumentTypeId);
        dbAccessor.executeUpdate(sql, newId, componentVersionId, documentTypeId, request.getDocumentName(),
                request.getUrl());
        responseObserver.onNext(AddDocumentResponse.newBuilder().setDocumentId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateDocument(UpdateDocumentRequest request, StreamObserver<UpdateDocumentResponse> responseObserver) {
        validateUpdateDocumentRequest(request);
        String sql = """
                UPDATE comp_documentation SET document_type_id = ?, document_name = ?, url = ?
                WHERE document_id = ? AND comp_vers_id = ?
                """;
        final Long documentTypeId = Helper.extract(request::hasDocumentTypeId, request::getDocumentTypeId);
        int affected = dbAccessor.executeUpdate(sql, documentTypeId, request.getDocumentName(), request.getUrl(),
                request.getDocumentId(), request.getComponentVersionId());
        responseObserver.onNext(UpdateDocumentResponse.newBuilder().setUpdatedCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDeliverablesList(Empty request, StreamObserver<GetDeliverablesListResponse> responseObserver) {
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "tcs_deliverables";
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<DeliverableProto> deliverables = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            DeliverableProto.Builder deliverible = DeliverableProto.newBuilder();
            Long resourceRoleId = Helper.getLong(data.get(i), "resource_role_id");
            if (resourceRoleId != null) {
                deliverible.setResourceRoleId(resourceRoleId);
            }
            Long phaseTypeId = Helper.getLong(data.get(i), "phase_type_id");
            if (phaseTypeId != null) {
                deliverible.setPhaseTypeId(phaseTypeId);
            }
            Long deliveribleId = Helper.getLong(data.get(i), "deliverable_id");
            if (deliveribleId != null) {
                deliverible.setDeliverableId(deliveribleId);
            }
            deliverables.add(deliverible.build());
        }
        responseObserver.onNext(GetDeliverablesListResponse.newBuilder().addAllDeliverables(deliverables).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isCockpitProjectUser(IsCockpitProjectUserRequest request,
            StreamObserver<IsCockpitProjectUserResponse> responseObserver) {
        validateIsCockpitProjectUserRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "cockpit_project_user";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("pj", String.valueOf(request.getProjectId()));
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        responseObserver
                .onNext(IsCockpitProjectUserResponse.newBuilder().setIsCockpitProjectUser(!data.isEmpty()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getCockpitProject(GetCockpitProjectRequest request,
            StreamObserver<GetCockpitProjectResponse> responseObserver) {
        validateGetCockpitProjectRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "cockpit_project_by_id";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("pj", String.valueOf(request.getCockpitProjectId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        GetCockpitProjectResponse response;
        if (data.isEmpty()) {
            response = GetCockpitProjectResponse.getDefaultInstance();
        } else {
            GetCockpitProjectResponse.Builder builder = GetCockpitProjectResponse.newBuilder();
            Long projectId = Helper.getLong(data.get(0), "tc_direct_project_id");
            if (projectId != null) {
                builder.setTcDirectProjectId(projectId);
            }
            String projectName = Helper.getString(data.get(0), "tc_direct_project_name");
            if (projectName != null) {
                builder.setTcDirectProjectName(projectName);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getAllCockpitProjects(Empty request, StreamObserver<GetAllCockpitProjectsResponse> responseObserver) {
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "cockpit_projects";
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<GetCockpitProjectResponse> projects = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            GetCockpitProjectResponse.Builder builder = GetCockpitProjectResponse.newBuilder();
            Long projectId = Helper.getLong(data.get(i), "tc_direct_project_id");
            if (projectId != null) {
                builder.setTcDirectProjectId(projectId);
            }
            String projectName = Helper.getString(data.get(i), "tc_direct_project_name");
            if (projectName != null) {
                builder.setTcDirectProjectName(projectName);
            }
            projects.add(builder.build());
        }
        responseObserver.onNext(GetAllCockpitProjectsResponse.newBuilder().addAllCockpitProjects(projects).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getCockpitProjectsForUser(GetCockpitProjectsForUserRequest request,
            StreamObserver<GetCockpitProjectsForUserResponse> responseObserver) {
        validateGetCockpitProjectsForUserRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "direct_my_projects";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<GetCockpitProjectResponse> projects = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            GetCockpitProjectResponse.Builder builder = GetCockpitProjectResponse.newBuilder();
            Long projectId = Helper.getLong(data.get(i), "tc_direct_project_id");
            if (projectId != null) {
                builder.setTcDirectProjectId(projectId);
            }
            String projectName = Helper.getString(data.get(i), "tc_direct_project_name");
            if (projectName != null) {
                builder.setTcDirectProjectName(projectName);
            }
            projects.add(builder.build());
        }
        responseObserver.onNext(GetCockpitProjectsForUserResponse.newBuilder().addAllCockpitProjects(projects).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getClientProject(GetClientProjectRequest request,
            StreamObserver<GetClientProjectResponse> responseObserver) {
        validateGetClientProjectRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "client_project_by_id";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("pj", String.valueOf(request.getClientProjectId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        GetClientProjectResponse response;
        if (data.isEmpty()) {
            response = GetClientProjectResponse.getDefaultInstance();
        } else {
            GetClientProjectResponse.Builder builder = GetClientProjectResponse.newBuilder();
            Long projectId = Helper.getLong(data.get(0), "project_id");
            if (projectId != null) {
                builder.setProjectId(projectId);
            }
            String projectName = Helper.getString(data.get(0), "project_name");
            if (projectName != null) {
                builder.setProjectName(projectName);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getAllClientProjects(Empty request, StreamObserver<GetAllClientProjectsResponse> responseObserver) {
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "client_projects";
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<GetClientProjectResponse> projects = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            GetClientProjectResponse.Builder builder = GetClientProjectResponse.newBuilder();
            Long projectId = Helper.getLong(data.get(i), "project_id");
            if (projectId != null) {
                builder.setProjectId(projectId);
            }
            String projectName = Helper.getString(data.get(i), "project_name");
            if (projectName != null) {
                builder.setProjectName(projectName);
            }
            projects.add(builder.build());
        }
        responseObserver.onNext(GetAllClientProjectsResponse.newBuilder().addAllClientProjects(projects).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getClientProjectsForUser(GetClientProjectsForUserRequest request,
            StreamObserver<GetClientProjectsForUserResponse> responseObserver) {
        validateGetClientProjectsForUserRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "client_projects_by_user";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<GetClientProjectResponse> projects = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            GetClientProjectResponse.Builder builder = GetClientProjectResponse.newBuilder();
            Long projectId = Helper.getLong(data.get(i), "project_id");
            if (projectId != null) {
                builder.setProjectId(projectId);
            }
            String projectName = Helper.getString(data.get(i), "project_name");
            if (projectName != null) {
                builder.setProjectName(projectName);
            }
            projects.add(builder.build());
        }
        responseObserver.onNext(GetClientProjectsForUserResponse.newBuilder().addAllClientProjects(projects).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchProjects(SearchProjectsRequest request, StreamObserver<SearchProjectsResponse> responseObserver) {
        validateSearchProjectsRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryNameForProject;
        String queryNameForProjectInfo;
        String paramName;
        switch (request.getParameterValue()) {
            case SearchProjectsParameter.STATUS_ID_VALUE: {
                queryNameForProject = "tcs_projects_by_status";
                queryNameForProjectInfo = "tcs_project_infos_by_status";
                paramName = "stid";
                break;
            }
            case SearchProjectsParameter.USER_ID_VALUE: {
                queryNameForProject = "tcs_projects_by_user";
                queryNameForProjectInfo = "tcs_project_infos_by_user";
                paramName = "uid";
                break;
            }
            default: {
                queryNameForProject = "tcs_projects_by_status";
                queryNameForProjectInfo = "tcs_project_infos_by_status";
                paramName = "stid";
                break;
            }
        }
        dbRequest.setContentHandle(queryNameForProject);
        dbRequest.setProperty(paramName, request.getValue());
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryNameForProject);
        List<ProjectProto> projects = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            ProjectProto.Builder builder = ProjectProto.newBuilder();
            builder.setProjectId(Helper.getLong(data.get(i), "project_id"));
            builder.setProjectCategoryId(Helper.getLong(data.get(i), "project_category_id"));
            builder.setProjectStatusId(Helper.getLong(data.get(i), "project_status_id"));
            builder.setCreateUser(Helper.getString(data.get(i), "create_user"));
            builder.setCreateDate(Timestamp.newBuilder()
                    .setSeconds(Helper.getDate(data.get(i), "create_date").toInstant().getEpochSecond()));
            builder.setModifyUser(Helper.getString(data.get(i), "modify_user"));
            builder.setModifyDate(Timestamp.newBuilder()
                    .setSeconds(Helper.getDate(data.get(i), "modify_date").toInstant().getEpochSecond()));
            projects.add(builder.build());
        }

        List<Map<String, Object>> projectInfosData = result.get(queryNameForProjectInfo);
        List<ProjectInfoProto> projectInfos = new ArrayList<>();
        for (int i = 0; i < projectInfosData.size(); i++) {
            ProjectInfoProto.Builder piBuilder = ProjectInfoProto.newBuilder();
            piBuilder.setProjectId(Helper.getLong(projectInfosData.get(i), "project_id"));
            piBuilder.setProjectInfoTypeId(Helper.getLong(projectInfosData.get(i), "project_info_type_id"));
            String value = Helper.getString(projectInfosData.get(i), "value");
            if (value != null) {
                piBuilder.setValue(value);
            }
            projectInfos.add(piBuilder.build());
        }
        responseObserver.onNext(
                SearchProjectsResponse.newBuilder().addAllProjects(projects).addAllProjectInfos(projectInfos).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectClient(GetProjectClientRequest request,
            StreamObserver<GetProjectClientResponse> responseObserver) {
        validateGetProjectClientRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsDwJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "non_admin_client_billing_accounts";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("tdpis", String.valueOf(request.getDirectProjectId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        long clientId = 0;
        if (data != null) {
            if (data.size() > 0) {
                clientId = Helper.getLong(data.get(0), "client_id");
            }
        }
        responseObserver.onNext(GetProjectClientResponse.newBuilder().setClientId(clientId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void checkUserChallengeEligibility(CheckUserChallengeEligibilityRequest request,
            StreamObserver<CheckUserChallengeEligibilityResponse> responseObserver) {
        validateCheckUserChallengeEligibilityRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getOltpJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "get_challenge_accessibility_and_groups";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("userId", String.valueOf(request.getUserId()));
        dbRequest.setProperty("challengeId", String.valueOf(request.getChallengeId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        CheckUserChallengeEligibilityResponse response;
        if (data != null && data.isEmpty()) {
            response = CheckUserChallengeEligibilityResponse.getDefaultInstance();
        } else {
            CheckUserChallengeEligibilityResponse.Builder builder = CheckUserChallengeEligibilityResponse.newBuilder();
            Long userGroupXrefFound = Helper.getLong(data.get(0), "user_group_xref_found");
            if (userGroupXrefFound != null) {
                builder.setUserGroupXrefFound(userGroupXrefFound);
            }
            Long challengeGroupInd = Helper.getLong(data.get(0), "challenge_group_ind");
            if (challengeGroupInd != null) {
                builder.setChallengeGroupInd(challengeGroupInd);
            }
            Long groupId = Helper.getLong(data.get(0), "group_id");
            if (groupId != null) {
                builder.setGroupId(groupId);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void searchProjectPhases(SearchProjectPhasesRequest request,
            StreamObserver<SearchProjectPhasesResponse> responseObserver) {
        validateSearchProjectPhasesRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName;
        String paramName;
        switch (request.getParameterValue()) {
            case SearchProjectsParameter.STATUS_ID_VALUE: {
                queryName = "tcs_project_phases_by_status";
                paramName = "stid";
                break;
            }
            case SearchProjectsParameter.USER_ID_VALUE: {
                queryName = "tcs_project_phases_by_user";
                paramName = "uid";
                break;
            }
            default: {
                queryName = "tcs_project_phases_by_status";
                paramName = "stid";
                break;
            }
        }
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty(paramName, request.getValue());
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<ProjectPhaseProto> phases = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            ProjectPhaseProto.Builder builder = ProjectPhaseProto.newBuilder();
            builder.setProjectId(Helper.getLong(data.get(i), "project_id"));
            builder.setProjectPhaseId(Helper.getLong(data.get(i), "project_phase_id"));
            builder.setDuration(Helper.getLong(data.get(i), "duration"));
            Date fixedStartTime = Helper.getDate(data.get(i), "fixed_start_time");
            if (fixedStartTime != null) {
                builder.setFixedStartTime(
                        Timestamp.newBuilder().setSeconds(fixedStartTime.toInstant().getEpochSecond()));
            }
            Date scheduledStartTime = Helper.getDate(data.get(i), "scheduled_start_time");
            if (scheduledStartTime != null) {
                builder.setScheduledStartTime(
                        Timestamp.newBuilder().setSeconds(scheduledStartTime.toInstant().getEpochSecond()));
            }
            Date scheduledEndTime = Helper.getDate(data.get(i), "scheduled_end_time");
            if (scheduledEndTime != null) {
                builder.setScheduledEndTime(
                        Timestamp.newBuilder().setSeconds(scheduledEndTime.toInstant().getEpochSecond()));
            }
            Date actualStartTime = Helper.getDate(data.get(i), "actual_start_time");
            if (actualStartTime != null) {
                builder.setActualStartTime(
                        Timestamp.newBuilder().setSeconds(actualStartTime.toInstant().getEpochSecond()));
            }
            Date actualEndTime = Helper.getDate(data.get(i), "actual_end_time");
            if (actualEndTime != null) {
                builder.setActualEndTime(
                        Timestamp.newBuilder().setSeconds(actualEndTime.toInstant().getEpochSecond()));
            }
            Long phaseStatusId = Helper.getLong(data.get(i), "phase_status_id");
            if (phaseStatusId != null) {
                builder.setPhaseStatusId(phaseStatusId);
            }
            Long phaseTypeId = Helper.getLong(data.get(i), "phase_type_id");
            if (phaseTypeId != null) {
                builder.setPhaseTypeId(phaseTypeId);
            }
            Long dependencyPhaseId = Helper.getLong(data.get(i), "dependency_phase_id");
            if (dependencyPhaseId != null) {
                builder.setDependencyPhaseId(dependencyPhaseId);
            }
            Long dependentPhaseId = Helper.getLong(data.get(i), "dependent_phase_id");
            if (dependentPhaseId != null) {
                builder.setDependentPhaseId(dependentPhaseId);
            }
            Long lagTime = Helper.getLong(data.get(i), "lag_time");
            if (lagTime != null) {
                builder.setLagTime(lagTime);
            }
            builder.setDependencyStart(Helper.getInt(data.get(i), "dependency_start") == 1);
            builder.setDependentStart(Helper.getInt(data.get(i), "dependent_start") == 1);
            phases.add(builder.build());
        }
        responseObserver.onNext(
                SearchProjectPhasesResponse.newBuilder().addAllPhases(phases).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchUserResourcesByUserId(SearchUserResourcesByUserIdRequest request,
            StreamObserver<SearchUserResourcesResponse> responseObserver) {
        validateSearchUserResourcesByUserIdRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "tcs_global_resources_by_user";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<Map<String, Object>> infoData = result.get("tcs_global_resource_infos_by_user");
        responseObserver.onNext(getResourcesResponse(data, infoData));
        responseObserver.onCompleted();
    }

    @Override
    public void searchUserResourcesByUserIdAndStatus(SearchUserResourcesByUserIdAndStatusRequest request,
            StreamObserver<SearchUserResourcesResponse> responseObserver) {
        validateSearchUserResourcesByUserIdAndStatusRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "tcs_resources_by_user_and_status";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        dbRequest.setProperty("stid", String.valueOf(request.getStatusId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<Map<String, Object>> infoData = result.get("tcs_resource_infos_by_user_and_status");
        responseObserver.onNext(getResourcesResponse(data, infoData));
        responseObserver.onCompleted();
    }

    private SearchUserResourcesResponse getResourcesResponse(List<Map<String, Object>> data,
            List<Map<String, Object>> infoData) {
        List<ResourceProto> resources = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            ResourceProto.Builder builder = ResourceProto.newBuilder();
            builder.setResourceId(Helper.getLong(data.get(i), "resource_id"));
            builder.setResourceRoleId(Helper.getLong(data.get(i), "resource_role_id"));
            Long projectId = Helper.getLong(data.get(i), "project_id");
            if (projectId != null) {
                builder.setProjectId(projectId);
            }
            Long phaseId = Helper.getLong(data.get(i), "phase_id");
            if (phaseId != null) {
                builder.setPhaseId(phaseId);
            }
            String createUser = Helper.getString(data.get(i), "create_user");
            if (createUser != null) {
                builder.setCreateUser(createUser);
            }
            Date createDate = Helper.getDate(data.get(i), "create_date");
            if (createDate != null) {
                builder.setCreateDate(Timestamp.newBuilder().setSeconds(createDate.toInstant().getEpochSecond()));
            }
            String modifyUser = Helper.getString(data.get(i), "modify_user");
            if (modifyUser != null) {
                builder.setModifyUser(modifyUser);
            }
            Date modifyDate = Helper.getDate(data.get(i), "modify_date");
            if (modifyDate != null) {
                builder.setModifyDate(Timestamp.newBuilder().setSeconds(modifyDate.toInstant().getEpochSecond()));
            }
            resources.add(builder.build());
        }
        List<ResourceInfoProto> resourceIfos = new ArrayList<>();
        for (int i = 0; i < infoData.size(); i++) {
            ResourceInfoProto.Builder builder = ResourceInfoProto.newBuilder();
            builder.setResourceId(Helper.getLong(infoData.get(i), "resource_id"));
            String propName = Helper.getString(infoData.get(i), "resource_info_type_name");
            if (propName != null) {
                builder.setResourceInfoTypeName(propName);
            }
            String value = Helper.getString(infoData.get(i), "value");
            if (value != null) {
                builder.setValue(value);
            }
            resourceIfos.add(builder.build());
        }
        return SearchUserResourcesResponse.newBuilder().addAllResources(resources).addAllResourceInfos(resourceIfos)
                .build();
    }

    private long generateNextCatalogScopedId() {
        try {
            if (!IdGenerator.isInitialized()) {
                IdGenerator.init(dbAccessor, dbAccessor.getTcsJdbcTemplate(), "sequence_object", "name",
                        "current_value", 9999999999L, 1, false);
            }
            return IdGenerator.nextId();
        } catch (DataAccessException e) {
            throw new RuntimeException("Failed to generate next catalog scoped ID", e);
        }
    }

    private void validateGetComponentVersionInfoRequest(GetComponentVersionInfoRequest request) {
        Helper.assertObjectNotNull(request::hasComponentId, "component_id");
        Helper.assertObjectNotNull(request::hasVersionNumber, "version_number");
    }

    private void validateGetDocumentsRequest(GetDocumentsRequest request) {
        Helper.assertObjectNotNull(request::hasComponentVersionId, "component_version_id");
    }

    private void validateAddDocumentRequest(AddDocumentRequest request) {
        Helper.assertObjectNotNull(request::hasDocumentName, "document_name");
        Helper.assertObjectNotNull(request::hasUrl, "url");
    }

    private void validateUpdateDocumentRequest(UpdateDocumentRequest request) {
        Helper.assertObjectNotNull(request::hasDocumentId, "document_id");
        Helper.assertObjectNotNull(request::hasDocumentName, "document_name");
        Helper.assertObjectNotNull(request::hasUrl, "url");
    }

    private void validateIsCockpitProjectUserRequest(IsCockpitProjectUserRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetCockpitProjectRequest(GetCockpitProjectRequest request) {
        Helper.assertObjectNotNull(request::hasCockpitProjectId, "cockpit_project_id");
    }

    private void validateGetCockpitProjectsForUserRequest(GetCockpitProjectsForUserRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetClientProjectRequest(GetClientProjectRequest request) {
        Helper.assertObjectNotNull(request::hasClientProjectId, "client_project_id");
    }

    private void validateGetClientProjectsForUserRequest(GetClientProjectsForUserRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateSearchProjectsRequest(SearchProjectsRequest request) {
        Helper.assertObjectNotNull(request::hasValue, "value");
    }

    private void validateGetProjectClientRequest(GetProjectClientRequest request) {
        Helper.assertObjectNotNull(request::hasDirectProjectId, "direct_project_id");
    }

    private void validateCheckUserChallengeEligibilityRequest(CheckUserChallengeEligibilityRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasChallengeId, "challenge_id");
    }

    private void validateSearchProjectPhasesRequest(SearchProjectPhasesRequest request) {
        Helper.assertObjectNotNull(request::hasValue, "value");
    }

    private void validateSearchUserResourcesByUserIdRequest(SearchUserResourcesByUserIdRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateSearchUserResourcesByUserIdAndStatusRequest(
            SearchUserResourcesByUserIdAndStatusRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasStatusId, "status_id");
    }
}
