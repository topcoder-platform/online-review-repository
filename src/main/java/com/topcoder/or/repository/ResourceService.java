package com.topcoder.or.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.util.SerializationUtils;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.component.id.DBHelper;
import com.topcoder.onlinereview.component.id.IDGenerator;
import com.topcoder.onlinereview.component.search.SearchBundle;
import com.topcoder.onlinereview.component.search.SearchBundleManager;
import com.topcoder.onlinereview.component.search.filter.Filter;
import com.topcoder.onlinereview.grpc.resource.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ResourceService extends ResourceServiceGrpc.ResourceServiceImplBase {
    private final DBAccessor dbAccessor;
    private final DBHelper dbHelper;
    private final SearchBundleManager searchBundleManager;

    private static final String RESOURCE_SEARCH_BUNDLE_NAME = "Resource Search Bundle";
    private static final String RESOURCE_ROLE_SEARCH_BUNDLE_NAME = "Resource Role Search Bundle";
    private static final String NOTIFICATION_SEARCH_BUNDLE_NAME = "Notification Search Bundle";
    private static final String NOTIFICATION_TYPE_SEARCH_BUNDLE_NAME = "Notification Type Search Bundle";

    private SearchBundle resourceSearchBundle;
    private SearchBundle resourceRoleSearchBundle;
    private SearchBundle notificationSearchBundle;
    private SearchBundle notificationTypeSearchBundle;
    private IDGenerator resourceIdGenerator;
    private IDGenerator resourceRoleIdGenerator;
    private IDGenerator notificationTypeIdGenerator;

    private String RESOURCE_ID_GENERATOR_NAME = "resource_id_seq";
    private String RESOURCE_ROLE_ID_GENERATOR_NAME = "resource_role_id_seq";
    private String NOTIFICATION_TYPE_ID_GENERATOR_NAME = "notification_type_id_seq";

    public ResourceService(DBAccessor dbAccessor, DBHelper dbHelper, SearchBundleManager searchBundleManager) {
        this.dbAccessor = dbAccessor;
        this.dbHelper = dbHelper;
        this.searchBundleManager = searchBundleManager;
    }

    @PostConstruct
    public void postRun() {
        notificationSearchBundle = searchBundleManager.getSearchBundle(NOTIFICATION_SEARCH_BUNDLE_NAME);
        resourceRoleSearchBundle = searchBundleManager.getSearchBundle(RESOURCE_ROLE_SEARCH_BUNDLE_NAME);
        resourceSearchBundle = searchBundleManager.getSearchBundle(RESOURCE_SEARCH_BUNDLE_NAME);
        notificationTypeSearchBundle = searchBundleManager.getSearchBundle(NOTIFICATION_TYPE_SEARCH_BUNDLE_NAME);
        resourceIdGenerator = new IDGenerator(RESOURCE_ID_GENERATOR_NAME, dbHelper);
        resourceRoleIdGenerator = new IDGenerator(RESOURCE_ROLE_ID_GENERATOR_NAME, dbHelper);
        notificationTypeIdGenerator = new IDGenerator(NOTIFICATION_TYPE_ID_GENERATOR_NAME, dbHelper);
    }

    @Override
    public void getResource(ResourceIdProto request, StreamObserver<ResourceProto> responseObserver) {
        validateResourceIdProto(request);
        String sql = """
                SELECT resource.resource_id, resource_role_id, project_id, project_phase_id, user_id,
                resource.create_user, resource.create_date, resource.modify_user, resource.modify_date
                FROM resource
                WHERE resource.resource_id = ?
                """;
        List<ResourceProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadResource(rs),
                request.getResourceId());
        responseObserver.onNext(result.isEmpty() ? ResourceProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getAllResources(ResourceIdsProto request, StreamObserver<GetResourcesResponse> responseObserver) {
        validateResourceIdsProto(request);
        String sql = """
                SELECT resource.resource_id, resource_role_id, project_id, project_phase_id, user_id,
                resource.create_user, resource.create_date, resource.modify_user, resource.modify_date
                FROM resource WHERE resource.resource_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getResourceIdsCount());
        List<ResourceProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> loadResource(rs),
                request.getResourceIdsList().toArray());
        responseObserver.onNext(GetResourcesResponse.newBuilder().addAllResources(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getResourcesByProjects(GetResourcesByProjectsRequest request,
            StreamObserver<GetResourcesResponse> responseObserver) {
        validateGetResourcesByProjectsRequest(request);
        String sql = """
                SELECT r.resource_id, r.resource_role_id, r.project_id, r.project_phase_id, r.user_id,
                r.create_user, r.create_date, r.modify_user, r.modify_date
                FROM resource r
                WHERE r.user_id = ? AND r.project_id IN (%s)
                    """;
        String inSql = Helper.getInClause(request.getProjectIdsCount());
        List<Object> param = new ArrayList<>();
        param.add(request.getUserId());
        param.addAll(request.getProjectIdsList());
        List<ResourceProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> loadResource(rs),
                param.toArray());
        responseObserver.onNext(GetResourcesResponse.newBuilder().addAllResources(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getResourceSubmissions(ResourceIdsProto request,
            StreamObserver<GetSubmissionsResponse> responseObserver) {
        validateResourceIdsProto(request);
        String sql = """
                SELECT resource_id, submission_id
                FROM resource_submission
                WHERE resource_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getResourceIdsCount());
        List<ResourceSubmissionProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ResourceSubmissionProto.Builder builder = ResourceSubmissionProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setSubmissionId);
            return builder.build();
        }, request.getResourceIdsList().toArray());
        responseObserver.onNext(GetSubmissionsResponse.newBuilder().addAllSubmissions(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getResourceInfo(ResourceIdProto request, StreamObserver<GetResourceInfoResponse> responseObserver) {
        validateResourceIdProto(request);
        String sql = """
                SELECT resource_info.resource_info_type_id, resource_info_type_lu.name, resource_info.value
                FROM resource_info, resource_info_type_lu
                WHERE resource_info.resource_info_type_id = resource_info_type_lu.resource_info_type_id AND resource_id = ?
                """;
        List<ResourceInfoProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ResourceInfoProto.Builder builder = ResourceInfoProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setResourceInfoTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setTypeName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setValue);
            return builder.build();
        }, request.getResourceId());
        responseObserver.onNext(GetResourceInfoResponse.newBuilder().addAllResourceInfo(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllResourceInfo(ResourceIdsProto request, StreamObserver<GetResourceInfoResponse> responseObserver) {
        validateResourceIdsProto(request);
        String sql = """
                SELECT resource_info.resource_id, resource_info.resource_info_type_id, resource_info_type_lu.name, resource_info.value
                FROM resource_info
                INNER JOIN resource_info_type_lu ON (resource_info.resource_info_type_id = resource_info_type_lu.resource_info_type_id)
                WHERE resource_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getResourceIdsCount());
        List<ResourceInfoProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ResourceInfoProto.Builder builder = ResourceInfoProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setResourceId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setResourceInfoTypeId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setTypeName);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setValue);
            return builder.build();
        }, request.getResourceIdsList().toArray());
        responseObserver.onNext(GetResourceInfoResponse.newBuilder().addAllResourceInfo(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getResourceInfoTypeId(GetResourceInfoTypeIdRequest request,
            StreamObserver<GetResourceInfoTypeIdResponse> responseObserver) {
        validateGetResourceInfoTypeIdRequest(request);
        String sql = """
                SELECT resource_info_type_id FROM resource_info_type_lu WHERE name = ?
                """;
        List<GetResourceInfoTypeIdResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetResourceInfoTypeIdResponse.Builder builder = GetResourceInfoTypeIdResponse.newBuilder();
            ResultSetHelper.applyResultSetInt(rs, 1, builder::setResourceInfoTypeId);
            return builder.build();
        }, request.getName());
        responseObserver.onNext(result.isEmpty() ? GetResourceInfoTypeIdResponse.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getResourceRoles(Empty request, StreamObserver<GetResourceRolesReponse> responseObserver) {
        String sql = """
                SELECT resource_role_id, phase_type_id, name, description, create_user, create_date, modify_user, modify_date
                FROM resource_role_lu
                """;
        List<ResourceRoleProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadResourceRole(rs));
        responseObserver.onNext(GetResourceRolesReponse.newBuilder().addAllResourceRoles(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNotification(NotificationProto request, StreamObserver<NotificationProto> responseObserver) {
        validateGetNotification(request);
        String sql = """
                SELECT project_id, external_ref_id, notification_type_id, create_user, create_date, modify_user, modify_date
                FROM notification
                WHERE project_id = ? AND external_ref_id = ? AND notification_type_id = ?
                """;
        List<NotificationProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadNotification(rs));
        responseObserver.onNext(result.isEmpty() ? NotificationProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getAllNotifications(GetAllNotificationsRequest request,
            StreamObserver<GetAllNotificationsResponse> responseObserver) {
        validateGetAllNotificationsRequest(request);
        String sql = """
                SELECT project_id, external_ref_id, notification_type_id, create_user, create_date, modify_user, modify_date
                FROM notification
                WHERE
                """;
        String condition = Helper.buildNStatement(request.getNotificationsCount(),
                "(project_id = ? AND external_ref_id = ? AND notification_type_id = ?)", " OR ");
        List<Object> param = new ArrayList<>();
        for (NotificationProto n : request.getNotificationsList()) {
            param.add(n.getProjectId());
            param.add(n.getExternalRefId());
            param.add(n.getNotificationTypeId());
        }
        List<NotificationProto> result = dbAccessor.executeQuery(sql.concat(condition),
                (rs, _i) -> loadNotification(rs), param.toArray());
        responseObserver.onNext(GetAllNotificationsResponse.newBuilder().addAllNotifications(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNotificationType(NotificationTypeIdProto request,
            StreamObserver<NotificationTypeProto> responseObserver) {
        validateNotificationTypeIdProto(request);
        String sql = """
                SELECT notification_type_id, name, description, create_user, create_date, modify_user, modify_date
                FROM notification_type_lu
                WHERE notification_type_id = ?
                """;
        List<NotificationTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadNotificationType(rs));
        responseObserver.onNext(result.isEmpty() ? NotificationTypeProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getAllNotificationType(NotificationTypeIdsProto request,
            StreamObserver<GetNotificationTypesResponse> responseObserver) {
        validateNotificationTypeIdsProto(request);
        String sql = """
                SELECT notification_type_id, name, description, create_user, create_date, modify_user, modify_date
                FROM notification_type_lu
                WHERE notification_type_id IN (%s);
                """;
        String inSql = Helper.getInClause(request.getNotificationTypeIdsCount());
        List<NotificationTypeProto> result = dbAccessor.executeQuery(sql.formatted(inSql),
                (rs, _i) -> loadNotificationType(rs));
        responseObserver.onNext(GetNotificationTypesResponse.newBuilder().addAllNotificationTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createResource(ResourceProto request, StreamObserver<ResourceIdProto> responseObserver) {
        validateCreateResource(request);
        long newId = resourceIdGenerator.getNextID();
        String sql = """
                INSERT INTO resource
                (resource_id, resource_role_id, project_id, project_phase_id, user_id, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final Long projectId = Helper.extract(request::hasProjectId, request::getProjectId);
        final Long phaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        dbAccessor.executeUpdate(sql, newId, request.getResourceRoleId(), projectId, phaseId, request.getUserId(),
                request.getCreateUser(), Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(ResourceIdProto.newBuilder().setResourceId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createResourceSubmission(ResourceSubmissionProto request, StreamObserver<CountProto> responseObserver) {
        validateCreateResourceSubmission(request);
        String sql = """
                INSERT INTO resource_submission(resource_id, submission_id, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getResourceId(), request.getSubmissionId(),
                request.getCreateUser(), Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createResourceInfo(ResourceInfoProto request, StreamObserver<CountProto> responseObserver) {
        validateCreateResourceInfo(request);
        String sql = """
                INSERT INTO resource_info(resource_id, resource_info_type_id, value, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getResourceId(), request.getResourceInfoTypeId(),
                request.getValue(), request.getCreateUser(), Helper.convertDate(request.getCreateDate()),
                request.getModifyUser(), Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createResourceRole(ResourceRoleProto request, StreamObserver<ResourceRoleIdProto> responseObserver) {
        validateCreateResourceRole(request);
        long newId = resourceRoleIdGenerator.getNextID();
        String sql = """
                INSERT INTO resource_role_lu(resource_role_id, name, description, phase_type_id, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final Long phaseTypeId = Helper.extract(request::hasPhaseTypeId, request::getPhaseTypeId);
        dbAccessor.executeUpdate(sql, newId, request.getName(), request.getDescription(), phaseTypeId,
                request.getCreateUser(), Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(ResourceRoleIdProto.newBuilder().setResourceRoleId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createNotification(NotificationProto request, StreamObserver<CountProto> responseObserver) {
        validateCreateNotification(request);
        String sql = """
                INSERT INTO notification (project_id, external_ref_id, notification_type_id, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getExternalRefId(),
                request.getNotificationTypeId(), request.getCreateUser(), Helper.convertDate(request.getCreateDate()),
                request.getModifyUser(), Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createNotificationType(NotificationTypeProto request,
            StreamObserver<NotificationTypeIdProto> responseObserver) {
        validateCreateNotificationType(request);
        long newId = notificationTypeIdGenerator.getNextID();
        String sql = """
                INSERT INTO notification_type_lu (notification_type_id, name, description, create_user, create_date, modify_user, modify_date)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, newId, request.getName(), request.getDescription(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(NotificationTypeIdProto.newBuilder().setNotificationTypeId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateResource(ResourceProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateResource(request);
        String sql = """
                UPDATE resource SET resource_role_id = ?, project_id = ?, project_phase_id = ?, user_id = ?, modify_user = ?, modify_date = ?
                WHERE resource_id = ?
                """;
        final Long projectId = Helper.extract(request::hasProjectId, request::getProjectId);
        final Long phaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        int affected = dbAccessor.executeUpdate(sql, request.getResourceRoleId(), projectId, phaseId,
                request.getUserId(), request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getResourceId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateResourceSubmission(ResourceSubmissionProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateResourceSubmission(request);
        String sql = """
                UPDATE resource_submission SET modify_user = ?, modify_date = ?
                WHERE resource_id = ? AND submission_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getResourceId(), request.getSubmissionId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateResourceInfo(ResourceInfoProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateResourceInfo(request);
        String sql = """
                UPDATE resource_info SET value = ? WHERE resource_id = ? AND resource_info_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getValue(), request.getResourceId(),
                request.getResourceInfoTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateResourceRole(ResourceRoleProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateResourceRole(request);
        String sql = """
                UPDATE resource_role_lu SET phase_type_id = ?, name = ?, description = ?, modify_user = ?, modify_date = ?
                WHERE resource_role_id = ?
                """;
        final Long phaseTypeId = Helper.extract(request::hasPhaseTypeId, request::getPhaseTypeId);
        int affected = dbAccessor.executeUpdate(sql, phaseTypeId, request.getName(), request.getDescription(),
                request.getModifyUser(), Helper.convertDate(request.getModifyDate()), request.getResourceRoleId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateNotificationType(NotificationTypeProto request, StreamObserver<CountProto> responseObserver) {
        validateUpdateNotificationType(request);
        String sql = """
                UPDATE notification_type_lu SET name = ?, description = ?, modify_user = ?, modify_date = ?  WHERE notification_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getName(), request.getDescription(),
                request.getModifyUser(), Helper.convertDate(request.getModifyDate()), request.getNotificationTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteResource(ResourceIdProto request, StreamObserver<CountProto> responseObserver) {
        validateResourceIdProto(request);
        deleteResourceInfo(request.getResourceId());
        deleteResourceSubmission(request.getResourceId());
        int affected = deleteResource(request.getResourceId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteResourceSubmission(ResourceSubmissionProto request, StreamObserver<CountProto> responseObserver) {
        validateDeleteResourceSubmission(request);
        String sql = """
                DELETE FROM resource_submission WHERE resource_id = ? AND submission_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getResourceId(), request.getSubmissionId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteResourceInfo(ResourceInfoProto request, StreamObserver<CountProto> responseObserver) {
        validateDeleteResourceInfo(request);
        String sql = """
                DELETE FROM resource_info WHERE resource_id = ? and resource_info_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getResourceId(), request.getResourceInfoTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteResourceRole(ResourceRoleIdProto request, StreamObserver<CountProto> responseObserver) {
        validateResourceRoleIdProto(request);
        String sql = """
                DELETE FROM resource_role_lu WHERE resource_role_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getResourceRoleId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteNotification(NotificationProto request, StreamObserver<CountProto> responseObserver) {
        validateDeleteNotification(request);
        String sql = """
                DELETE FROM notification WHERE project_id = ? AND external_ref_id = ? AND notification_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getExternalRefId(),
                request.getNotificationTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteNotificationType(NotificationTypeIdProto request, StreamObserver<CountProto> responseObserver) {
        validateNotificationTypeIdProto(request);
        String sql = """
                DELETE FROM notification_type_lu WHERE notification_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getNotificationTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void auditProjectUser(AuditProjectUserRequest request, StreamObserver<CountProto> responseObserver) {
        validateAuditProjectUserRequest(request);
        String sql = """
                INSERT INTO project_user_audit
                (project_user_audit_id, project_id, resource_user_id, resource_role_id, audit_action_type_id, action_date, action_user_id)
                VALUES (PROJECT_USER_AUDIT_SEQ.nextval, ?, ?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getResourceUserId(),
                request.getResourceRoleId(), request.getAuditActionTypeId(),
                Helper.convertDate(request.getActionDate()), request.getActionUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchResources(FilterProto request, StreamObserver<GetResourcesResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<ResourceProto> result = resourceSearchBundle.search(filter, (rs, _i) -> loadResource(rs));
        responseObserver.onNext(GetResourcesResponse.newBuilder().addAllResources(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchResourceRoles(FilterProto request, StreamObserver<GetResourceRolesReponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<ResourceRoleProto> result = resourceRoleSearchBundle.search(filter, (rs, _i) -> loadResourceRole(rs));
        responseObserver.onNext(GetResourceRolesReponse.newBuilder().addAllResourceRoles(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchNotifications(FilterProto request, StreamObserver<GetAllNotificationsResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<NotificationProto> result = notificationSearchBundle.search(filter, (rs, _i) -> loadNotification(rs));
        responseObserver.onNext(GetAllNotificationsResponse.newBuilder().addAllNotifications(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchNotificationTypes(FilterProto request,
            StreamObserver<GetNotificationTypesResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<NotificationTypeProto> result = notificationTypeSearchBundle.search(filter,
                (rs, _i) -> loadNotificationType(rs));
        responseObserver.onNext(GetNotificationTypesResponse.newBuilder().addAllNotificationTypes(result).build());
        responseObserver.onCompleted();
    }

    private int deleteResourceInfo(long resourceId) {
        String sql = """
                DELETE FROM resource_info WHERE resource_id = ?
                """;
        return dbAccessor.executeUpdate(sql, resourceId);
    }

    private int deleteResourceSubmission(long resourceId) {
        String sql = """
                DELETE FROM resource_submission WHERE resource_id = ?
                """;
        return dbAccessor.executeUpdate(sql, resourceId);
    }

    private int deleteResource(long resourceId) {
        String sql = """
                DELETE FROM resource WHERE resource_id = ?
                """;
        return dbAccessor.executeUpdate(sql, resourceId);
    }

    private ResourceProto loadResource(ResultSet rs) throws SQLException {
        ResourceProto.Builder builder = ResourceProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "resource_id", builder::setResourceId);
        ResultSetHelper.applyResultSetLong(rs, "resource_role_id", builder::setResourceRoleId);
        ResultSetHelper.applyResultSetLong(rs, "project_id", builder::setProjectId);
        ResultSetHelper.applyResultSetLong(rs, "project_phase_id", builder::setProjectPhaseId);
        ResultSetHelper.applyResultSetLong(rs, "user_id", builder::setUserId);
        ResultSetHelper.applyResultSetString(rs, "create_user", builder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "modify_user", builder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "modify_date", builder::setModifyDate);
        return builder.build();
    }

    private ResourceRoleProto loadResourceRole(ResultSet rs) throws SQLException {
        ResourceRoleProto.Builder builder = ResourceRoleProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "resource_role_id", builder::setResourceRoleId);
        ResultSetHelper.applyResultSetLong(rs, "phase_type_id", builder::setPhaseTypeId);
        ResultSetHelper.applyResultSetString(rs, "name", builder::setName);
        ResultSetHelper.applyResultSetString(rs, "description", builder::setDescription);
        ResultSetHelper.applyResultSetString(rs, "create_user", builder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "modify_user", builder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "modify_date", builder::setModifyDate);
        return builder.build();
    }

    private NotificationProto loadNotification(ResultSet rs) throws SQLException {
        NotificationProto.Builder builder = NotificationProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "project_id", builder::setProjectId);
        ResultSetHelper.applyResultSetLong(rs, "external_ref_id", builder::setExternalRefId);
        ResultSetHelper.applyResultSetLong(rs, "notification_type_id", builder::setNotificationTypeId);
        ResultSetHelper.applyResultSetString(rs, "create_user", builder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "modify_user", builder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "modify_date", builder::setModifyDate);
        return builder.build();
    }

    private NotificationTypeProto loadNotificationType(ResultSet rs) throws SQLException {
        NotificationTypeProto.Builder builder = NotificationTypeProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "notification_type_id", builder::setNotificationTypeId);
        ResultSetHelper.applyResultSetString(rs, "name", builder::setName);
        ResultSetHelper.applyResultSetString(rs, "description", builder::setDescription);
        ResultSetHelper.applyResultSetString(rs, "create_user", builder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "modify_user", builder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "modify_date", builder::setModifyDate);
        return builder.build();
    }

    private void validateResourceIdProto(ResourceIdProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
    }

    private void validateResourceIdsProto(ResourceIdsProto request) {
        Helper.assertObjectNotEmpty(request::getResourceIdsCount, "resource_ids");
    }

    private void validateGetResourcesByProjectsRequest(GetResourcesByProjectsRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotEmpty(request::getProjectIdsCount, "project_ids");
    }

    private void validateGetResourceInfoTypeIdRequest(GetResourceInfoTypeIdRequest request) {
        Helper.assertObjectNotNull(request::hasName, "name");
    }

    private void validateGetNotification(NotificationProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasExternalRefId, "external_ref_id");
        Helper.assertObjectNotNull(request::hasNotificationTypeId, "notification_type_id");
    }

    private void validateGetAllNotificationsRequest(GetAllNotificationsRequest request) {
        Helper.assertObjectNotEmpty(request::getNotificationsCount, "notifications");
        for (NotificationProto n : request.getNotificationsList()) {
            validateGetNotification(n);
        }
    }

    private void validateNotificationTypeIdProto(NotificationTypeIdProto request) {
        Helper.assertObjectNotNull(request::hasNotificationTypeId, "notification_type_id");
    }

    private void validateNotificationTypeIdsProto(NotificationTypeIdsProto request) {
        Helper.assertObjectNotEmpty(request::getNotificationTypeIdsCount, "notification_type_ids");
    }

    private void validateCreateResource(ResourceProto request) {
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateResourceSubmission(ResourceSubmissionProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasSubmissionId, "submission_id");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateResourceInfo(ResourceInfoProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasResourceInfoTypeId, "resource_info_type_id");
        Helper.assertObjectNotNull(request::hasValue, "value");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateResourceRole(ResourceRoleProto request) {
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateNotification(NotificationProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasExternalRefId, "external_ref_id");
        Helper.assertObjectNotNull(request::hasNotificationTypeId, "notification_type_id");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateCreateNotificationType(NotificationTypeProto request) {
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateResource(ResourceProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateResourceSubmission(ResourceSubmissionProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasSubmissionId, "submission_id");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateResourceInfo(ResourceInfoProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasResourceInfoTypeId, "resource_info_type_id");
        Helper.assertObjectNotNull(request::hasValue, "value");
    }

    private void validateUpdateResourceRole(ResourceRoleProto request) {
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateNotificationType(NotificationTypeProto request) {
        Helper.assertObjectNotNull(request::hasNotificationTypeId, "notification_type_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateDeleteResourceSubmission(ResourceSubmissionProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasSubmissionId, "submission_id");
    }

    private void validateDeleteResourceInfo(ResourceInfoProto request) {
        Helper.assertObjectNotNull(request::hasResourceId, "resource_id");
        Helper.assertObjectNotNull(request::hasResourceInfoTypeId, "resource_info_type_id");
    }

    private void validateResourceRoleIdProto(ResourceRoleIdProto request) {
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
    }

    private void validateDeleteNotification(NotificationProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasExternalRefId, "external_ref_id");
        Helper.assertObjectNotNull(request::hasNotificationTypeId, "notification_type_id");
    }

    private void validateAuditProjectUserRequest(AuditProjectUserRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasResourceUserId, "resource_user_id");
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
        Helper.assertObjectNotNull(request::hasAuditActionTypeId, "audit_action_type_id");
        Helper.assertObjectNotNull(request::hasActionDate, "action_date");
        Helper.assertObjectNotNull(request::hasActionUserId, "action_user_id");
    }
}
