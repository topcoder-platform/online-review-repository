package com.topcoder.or.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.component.shared.dataaccess.DataAccess;
import com.topcoder.onlinereview.component.shared.dataaccess.Request;
import com.topcoder.onlinereview.grpc.security.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class SecurityService extends SecurityServiceGrpc.SecurityServiceImplBase {
    private final DBAccessor dbAccessor;

    public SecurityService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void getGroupMembers(GetGroupMembersRequest request,
            StreamObserver<GetGroupMembersResponse> responseObserver) {
        validateGetGroupMembersRequest(request);
        String sql = """
                select gm.group_member_id, gm.user_id, gm.group_id, gm.use_group_default, gm.specific_permission, gm.active,
                gm.activated_on, gm.unassigned_on, cg.name, cg.default_permission, cg.client_id, cg.archived, cg.archived_on,
                cg.effective_group_id, cg.auto_grant
                from group_member gm
                join customer_group cg on gm.group_id=cg.group_id
                where gm.user_id=? and gm.active=1 and cg.archived=0
                """;
        List<GroupMemberProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GroupMemberProto.Builder builder = GroupMemberProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setGroupMemberId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setUserId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setGroupId);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setUseGroupDefault);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setSpecificPermission);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setActive);
            ResultSetHelper.applyResultSetTimestamp(rs, 7, builder::setActivatedOn);
            ResultSetHelper.applyResultSetTimestamp(rs, 8, builder::setUnassignedOn);
            ResultSetHelper.applyResultSetString(rs, 9, builder::setGroupName);
            ResultSetHelper.applyResultSetString(rs, 10, builder::setGroupDefaultPermission);
            ResultSetHelper.applyResultSetLong(rs, 11, builder::setGroupClientId);
            ResultSetHelper.applyResultSetBool(rs, 12, builder::setGroupArchived);
            ResultSetHelper.applyResultSetTimestamp(rs, 13, builder::setGroupArchivedOn);
            ResultSetHelper.applyResultSetLong(rs, 14, builder::setGroupEffectiveGroupId);
            ResultSetHelper.applyResultSetBool(rs, 15, builder::setGroupAutoGrant);
            return builder.build();
        }, request.getUserId());
        responseObserver.onNext(GetGroupMembersResponse.newBuilder().addAllGroupMembers(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDirectProjects(GetDirectProjectsRequest request,
            StreamObserver<GetDirectProjectsResponse> responseObserver) {
        validateGetDirectProjectsRequest(request);
        String sql = """
                select group_id, group_direct_project_id, tc_direct_project_id
                from group_associated_direct_projects
                where group_id=?
                """;
        List<GroupDirectProjectProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GroupDirectProjectProto.Builder builder = GroupDirectProjectProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setGroupId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setGroupDirectProjectId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setTcDirectProjectId);
            return builder.build();
        }, request.getGroupId());
        responseObserver.onNext(GetDirectProjectsResponse.newBuilder().addAllDirectProjects(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getBillingAccounts(GetBillingAccountsRequest request,
            StreamObserver<GetBillingAccountsResponse> responseObserver) {
        validateGetBillingAccountsRequest(request);
        String sql = """
                select tp.project_id, tp.company_id, tp.name, tp.description, tp.client_id, tp.is_deleted
                from group_associated_billing_accounts gaba
                join tt_project tp on gaba.billing_account_id=tp.project_id
                where gaba.group_id=?
                """;
        List<BillingAccountProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            BillingAccountProto.Builder builder = BillingAccountProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setCompanyId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setDescription);
            ResultSetHelper.applyResultSetLong(rs, 5, builder::setClientId);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setIsDeleted);
            return builder.build();
        }, request.getGroupId());
        responseObserver.onNext(GetBillingAccountsResponse.newBuilder().addAllBillingAccounts(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isCustomerAdministrator(IsCustomerAdministratorRequest request,
            StreamObserver<IsCustomerAdministratorResponse> responseObserver) {
        validateIsCustomerAdministratorRequest(request);
        String sql;
        List<Object> params = new ArrayList<>();
        params.add(request.getUserId());
        if (request.hasClientId()) {
            sql = """
                    SELECT CASE WHEN EXISTS (SELECT 1 FROM customer_administrator WHERE user_id = ? and client_id = ?) THEN 1 ELSE 0 END FROM DUAL
                    """;
            params.add(request.getClientId());
        } else {
            sql = """
                    SELECT CASE WHEN EXISTS (SELECT 1 FROM customer_administrator WHERE user_id = ?) THEN 1 ELSE 0 END FROM DUAL
                    """;
        }
        boolean result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, params.toArray()).get(0);
        responseObserver
                .onNext(IsCustomerAdministratorResponse.newBuilder().setIsCustomerAdministrator(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isAdministrator(IsAdministratorRequest request,
            StreamObserver<IsAdministratorResponse> responseObserver) {
        validateIsAdministratorRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "is_user_tc_staff";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> userData = result.get(queryName);
        IsAdministratorResponse response = IsAdministratorResponse.newBuilder().setIsAdministrator(!userData.isEmpty())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getGroupIdsOfFullPermissionsUser(GetGroupIdsOfFullPermissionsUserRequest request,
            StreamObserver<GetGroupIdsOfFullPermissionsUserResponse> responseObserver) {
        validateGetGroupIdsOfFullPermissionsUserRequest(request);
        String sql = """
                SELECT cg.group_id
                FROM group_member gm
                LEFT JOIN customer_group cg on gm.group_id=cg.group_id
                WHERE ((gm.use_group_default = 0 AND gm.specific_permission = 'FULL')
                OR (gm.use_group_default = 1 AND cg.default_permission = 'FULL'))
                AND cg.archived = 0 AND gm.active = 1 AND gm.user_id=?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getUserId());
        responseObserver.onNext(GetGroupIdsOfFullPermissionsUserResponse.newBuilder().addAllGroupIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getBillingAccount(GetBillingAccountRequest request,
            StreamObserver<GetBillingAccountResponse> responseObserver) {
        validateGetBillingAccountRequest(request);
        String sql = """
                select tp.project_id, tp.company_id, tp.name, tp.description, tp.client_id, tp.is_deleted,
                c.company_id as client_company_id, c.name as client_name, c.is_deleted as client_is_deleted
                from tt_project tp
                left join client c on c.client_id = tp.client_id
                where tp.project_id = ? and tp.is_deleted = 0
                """;
        List<GetBillingAccountResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetBillingAccountResponse.Builder builder = GetBillingAccountResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setCompanyId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setDescription);
            ResultSetHelper.applyResultSetLong(rs, 5, builder::setClientId);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setIsDeleted);
            ResultSetHelper.applyResultSetLong(rs, 7, builder::setClientCompanyId);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setClientName);
            ResultSetHelper.applyResultSetBool(rs, 9, builder::setClientIsDeleted);
            return builder.build();
        }, request.getId());
        responseObserver.onNext(result.isEmpty() ? GetBillingAccountResponse.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getBillingAccountsForClient(Empty request,
            StreamObserver<GetBillingAccountsForClientResponse> responseObserver) {
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "admin_client_billing_accounts_v2";
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<BillingAccountForClientProto> billingAccounts = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            BillingAccountForClientProto.Builder builder = BillingAccountForClientProto.newBuilder();
            Long clientId = Helper.getLong(data.get(i), "client_id");
            if (clientId != null) {
                builder.setClientId(clientId);
            }
            Long billingAccountId = Helper.getLong(data.get(i), "billing_account_id");
            if (billingAccountId != null) {
                builder.setBillingAccountId(billingAccountId);
            }
            String clientName = Helper.getString(data.get(i), "client_name");
            if (clientName != null) {
                builder.setClientName(clientName);
            }
            String billingAccountName = Helper.getString(data.get(i), "billing_account_name");
            if (billingAccountName != null) {
                builder.setBllingAccountName(billingAccountName);
            }
            billingAccounts.add(builder.build());
        }
        responseObserver.onNext(
                GetBillingAccountsForClientResponse.newBuilder().addAllBillingAccounts(billingAccounts).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectName(GetProjectNameRequest request, StreamObserver<GetProjectNameResponse> responseObserver) {
        validateGetProjectNameRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "project_name";
        dbRequest.setProperty("tcdirectid", String.valueOf(request.getProjectId()));
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        GetProjectNameResponse response;
        if (data.isEmpty()) {
            response = GetProjectNameResponse.getDefaultInstance();
        } else {
            GetProjectNameResponse.Builder builder = GetProjectNameResponse.newBuilder();
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
    public void getProjectsByClientId(Empty request, StreamObserver<GetProjectsByClientIdResponse> responseObserver) {
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "admin_client_billing_accounts_v2";
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<ProjectByClientIdProto> projects = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            ProjectByClientIdProto.Builder builder = ProjectByClientIdProto.newBuilder();
            Long clientId = Helper.getLong(data.get(i), "client_id");
            if (clientId != null) {
                builder.setClientId(clientId);
            }
            Long directProjectId = Helper.getLong(data.get(i), "direct_project_id");
            if (directProjectId != null) {
                builder.setDirectProjectId(directProjectId);
            }
            String directProjectName = Helper.getString(data.get(i), "direct_project_name");
            if (directProjectName != null) {
                builder.setDirectProjectName(directProjectName);
            }
            projects.add(builder.build());
        }
        responseObserver.onNext(GetProjectsByClientIdResponse.newBuilder().addAllProjects(projects).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectsByBillingAccounts(Empty request,
            StreamObserver<GetProjectsByBillingAccountsResponse> responseObserver) {
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getTcsJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "admin_client_billing_accounts_v2";
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> data = result.get(queryName);
        List<ProjectByBillingAccountProto> projects = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            ProjectByBillingAccountProto.Builder builder = ProjectByBillingAccountProto.newBuilder();
            Long directProjectId = Helper.getLong(data.get(i), "direct_project_id");
            if (directProjectId != null) {
                builder.setDirectProjectId(directProjectId);
            }
            Long billingAccountId = Helper.getLong(data.get(i), "billing_account_id");
            if (billingAccountId != null) {
                builder.setBillingAccountId(billingAccountId);
            }
            String directProjectName = Helper.getString(data.get(i), "direct_project_name");
            if (directProjectName != null) {
                builder.setDirectProjectName(directProjectName);
            }
            projects.add(builder.build());
        }
        responseObserver.onNext(GetProjectsByBillingAccountsResponse.newBuilder().addAllProjects(projects).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUserRoles(GetUserRolesRequest request, StreamObserver<GetUserRolesResponse> responseObserver) {
        validateGetUserRolesRequest(request);
        String sql = """
                SELECT security_roles.role_id, description
                FROM user_role_xref, security_roles
                WHERE user_role_xref.login_id = ? AND user_role_xref.role_id = security_roles.role_id
                AND user_role_xref.security_status_id = 1
                UNION
                SELECT security_roles.role_id, description
                FROM security_roles, user_group_xref, group_role_xref
                WHERE user_group_xref.login_id = ? AND user_group_xref.group_id = group_role_xref.group_id
                AND user_group_xref.security_status_id = 1 AND group_role_xref.security_status_id = 1
                AND group_role_xref.role_id = security_roles.role_id
                """;
        List<UserRoleProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            UserRoleProto.Builder builder = UserRoleProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setRoleId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDescription);
            return builder.build();
        }, request.getUserId(), request.getUserId());
        responseObserver.onNext(GetUserRolesResponse.newBuilder().addAllUserRoles(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isCloudSpokesUser(IsCloudSpokesUserRequest request,
            StreamObserver<IsCloudSpokesUserResponse> responseObserver) {
        validateIsCloudSpokesUserRequest(request);
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM user WHERE handle = ? and lower(reg_source) = 'cloudspokes') THEN 1 ELSE 0 END FROM DUAL
                """;
        boolean result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, request.getHandle()).get(0);
        responseObserver.onNext(IsCloudSpokesUserResponse.newBuilder().setIsCloudSpokesUser(result).build());
        responseObserver.onCompleted();
    }

    private void validateGetGroupMembersRequest(GetGroupMembersRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetDirectProjectsRequest(GetDirectProjectsRequest request) {
        Helper.assertObjectNotNull(request::hasGroupId, "group_id");
    }

    private void validateGetBillingAccountsRequest(GetBillingAccountsRequest request) {
        Helper.assertObjectNotNull(request::hasGroupId, "group_id");
    }

    private void validateIsCustomerAdministratorRequest(IsCustomerAdministratorRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateIsAdministratorRequest(IsAdministratorRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetGroupIdsOfFullPermissionsUserRequest(GetGroupIdsOfFullPermissionsUserRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetBillingAccountRequest(GetBillingAccountRequest request) {
        Helper.assertObjectNotNull(request::hasId, "id");
    }

    private void validateGetProjectNameRequest(GetProjectNameRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateGetUserRolesRequest(GetUserRolesRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateIsCloudSpokesUserRequest(IsCloudSpokesUserRequest request) {
        Helper.assertObjectNotNull(request::hasHandle, "handle");
    }
}
