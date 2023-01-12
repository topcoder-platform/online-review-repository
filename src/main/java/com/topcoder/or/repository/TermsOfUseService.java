package com.topcoder.or.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.PostConstruct;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.component.id.DBHelper;
import com.topcoder.onlinereview.component.id.IDGenerator;
import com.topcoder.onlinereview.grpc.termsofuse.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class TermsOfUseService extends TermsOfUseServiceGrpc.TermsOfUseServiceImplBase {
    private final DBAccessor dbAccessor;
    private final DBHelper dbHelper;

    private IDGenerator idGenerator;

    private static final String TERMS_OF_USE_SEQ = "TERMS_OF_USE_SEQ";

    public TermsOfUseService(DBAccessor dbAccessor, DBHelper dbHelper) {
        this.dbAccessor = dbAccessor;
        this.dbHelper = dbHelper;
    }

    @PostConstruct
    public void postRun() {
        idGenerator = new IDGenerator(TERMS_OF_USE_SEQ, dbHelper);
    }

    @Override
    public void createProjectRoleTermsOfUse(CreateProjectRoleTermsOfUseRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateCreateProjectRoleTermsOfUseRequest(request);
        String sql = """
                INSERT INTO project_role_terms_of_use_xref (project_id, resource_role_id, terms_of_use_id, sort_order, group_ind)
                VALUES (?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getResourceRoleId(),
                request.getTermsOfUseId(), request.getSortOrder(), request.getGroupInd());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteProjectRoleTermsOfUse(DeleteProjectRoleTermsOfUseRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeleteProjectRoleTermsOfUseRequest(request);
        String sql = """
                DELETE FROM project_role_terms_of_use_xref WHERE project_id = ? and resource_role_id = ? and terms_of_use_id = ? and group_ind = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getResourceRoleId(),
                request.getTermsOfUseId(), request.getGroupInd());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectRoleTermsOfUse(GetProjectRoleTermsOfUseRequest request,
            StreamObserver<GetProjectRoleTermsOfUseResponse> responseObserver) {
        validateGetProjectRoleTermsOfUseRequest(request);
        String sql = """
                SELECT tou.terms_of_use_id, sort_order, group_ind, terms_of_use_type_id, title, url, tou.terms_of_use_agreeability_type_id,
                touat.name as terms_of_use_agreeability_type_name, touat.description as terms_of_use_agreeability_type_description
                FROM project_role_terms_of_use_xref
                INNER JOIN terms_of_use tou ON project_role_terms_of_use_xref.terms_of_use_id = tou.terms_of_use_id
                INNER JOIN terms_of_use_agreeability_type_lu touat ON touat.terms_of_use_agreeability_type_id = tou.terms_of_use_agreeability_type_id
                WHERE project_id = ? AND resource_role_id = ?
                order by group_ind, sort_order
                """;
        List<ProjectRoleTermsOfUseProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectRoleTermsOfUseProto.Builder builder = ProjectRoleTermsOfUseProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setTermsOfUseId);
            ResultSetHelper.applyResultSetInt(rs, 2, builder::setSortOrder);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setGroupInd);
            ResultSetHelper.applyResultSetInt(rs, 4, builder::setTermsOfUseTypeId);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setTitle);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setUrl);
            ResultSetHelper.applyResultSetInt(rs, 7, builder::setTermsOfUseAgreeabilityTypeId);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setTermsOfUseAgreeabilityTypeName);
            ResultSetHelper.applyResultSetString(rs, 9, builder::setTermsOfUseAgreeabilityTypeDescription);
            return builder.build();
        }, request.getProjectId(), request.getResourceRoleId());
        responseObserver
                .onNext(GetProjectRoleTermsOfUseResponse.newBuilder().addAllProjectRoleTermsOfUses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteAllProjectRoleTermsOfUse(ProjectIdProto request, StreamObserver<CountProto> responseObserver) {
        validateProjectIdProto(request);
        String sql = """
                DELETE FROM project_role_terms_of_use_xref WHERE project_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createTermsOfUse(CreateTermsOfUseRequest request, StreamObserver<TermsOfUseIdProto> responseObserver) {
        validateCreateTermsOfUseRequest(request);
        String sql = """
                INSERT INTO terms_of_use (terms_of_use_id, terms_text, terms_of_use_type_id, title, url, terms_of_use_agreeability_type_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        long newId = idGenerator.getNextID();
        String termsText = Helper.extract(request::hasTermsText, request::getTermsText);
        byte[] termsTextBytes = termsText == null ? null : termsText.getBytes();
        String url = Helper.extract(request::hasUrl, request::getUrl);
        dbAccessor.executeUpdate(sql, newId, termsTextBytes, request.getTermsOfUseTypeId(),
                request.getTitle(), url, request.getTermsOfUseAgreeabilityTypeId());
        responseObserver.onNext(TermsOfUseIdProto.newBuilder().setTermsOfUseId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateTermsOfUse(UpdateTermsOfUseRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateTermsOfUseRequest(request);
        String sql = """
                UPDATE terms_of_use
                SET terms_of_use_type_id = ?, title = ?, url = ?, terms_of_use_agreeability_type_id=?
                WHERE terms_of_use_id = ?
                """;
        String url = Helper.extract(request::hasUrl, request::getUrl);
        int affected = dbAccessor.executeUpdate(sql, request.getTermsOfUseTypeId(), request.getTitle(), url,
                request.getTermsOfUseAgreeabilityTypeId(), request.getTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTermsOfUse(TermsOfUseIdProto request, StreamObserver<GetTermsOfUseResponse> responseObserver) {
        validateTermsOfUseIdProto(request);
        String sql = """
                SELECT terms_of_use_id, terms_of_use_type_id, title, url, tou.terms_of_use_agreeability_type_id, touat.name as terms_of_use_agreeability_type_name,
                touat.description as terms_of_use_agreeability_type_description
                FROM terms_of_use tou
                INNER JOIN terms_of_use_agreeability_type_lu touat ON touat.terms_of_use_agreeability_type_id = tou.terms_of_use_agreeability_type_id
                WHERE terms_of_use_id=?
                """;
        List<GetTermsOfUseResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadTermsOfUse(rs),
                request.getTermsOfUseId());
        responseObserver.onNext(result.isEmpty() ? GetTermsOfUseResponse.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTermsOfUse(TermsOfUseIdProto request, StreamObserver<CountProto> responseObserver) {
        validateTermsOfUseIdProto(request);
        String sql = """
                DELETE FROM terms_of_use WHERE terms_of_use_id=?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTermsOfUseByTypeId(TermsOfUseTypeIdProto request,
            StreamObserver<GetTermsOfUseByTypeIdResponse> responseObserver) {
        validateTermsOfUseTypeIdProto(request);
        String sql = """
                SELECT terms_of_use_id, terms_of_use_type_id, title, url, tou.terms_of_use_agreeability_type_id, touat.name as terms_of_use_agreeability_type_name,
                touat.description as terms_of_use_agreeability_type_description
                FROM terms_of_use tou
                INNER JOIN terms_of_use_agreeability_type_lu touat ON touat.terms_of_use_agreeability_type_id = tou.terms_of_use_agreeability_type_id
                WHERE terms_of_use_type_id=?
                """;
        List<GetTermsOfUseResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadTermsOfUse(rs),
                request.getTermsOfUseTypeId());
        responseObserver.onNext(GetTermsOfUseByTypeIdResponse.newBuilder().addAllTermsOfUses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllTermsOfUse(Empty request, StreamObserver<GetAllTermsOfUseResponse> responseObserver) {
        String sql = """
                SELECT terms_of_use_id, terms_of_use_type_id, title, url, tou.terms_of_use_agreeability_type_id, touat.name as terms_of_use_agreeability_type_name,
                touat.description as terms_of_use_agreeability_type_description
                FROM terms_of_use tou
                INNER JOIN terms_of_use_agreeability_type_lu touat ON touat.terms_of_use_agreeability_type_id = tou.terms_of_use_agreeability_type_id
                """;
        List<GetTermsOfUseResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadTermsOfUse(rs));
        responseObserver.onNext(GetAllTermsOfUseResponse.newBuilder().addAllTermsOfUses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTermsOfUseType(TermsOfUseTypeIdProto request, StreamObserver<TermsOfUseTypeProto> responseObserver) {
        validateTermsOfUseTypeIdProto(request);
        String sql = """
                SELECT terms_of_use_type_id, terms_of_use_type_desc
                FROM terms_of_use_type
                WHERE terms_of_use_type_id=?
                """;
        List<TermsOfUseTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            TermsOfUseTypeProto.Builder builder = TermsOfUseTypeProto.newBuilder();
            ResultSetHelper.applyResultSetInt(rs, 1, builder::setTermsOfUseTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setTermsOfUseTypeDesc);
            return builder.build();
        }, request.getTermsOfUseTypeId());
        responseObserver.onNext(result.isEmpty() ? TermsOfUseTypeProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void updateTermsOfUseType(TermsOfUseTypeProto request, StreamObserver<CountProto> responseObserver) {
        validateTermsOfUseTypeProto(request);
        String sql = """
                UPDATE terms_of_use_type
                SET terms_of_use_type_desc = ?
                WHERE terms_of_use_type_id = ?
                """;
        String termsOfUseTypeDesc = Helper.extract(request::hasTermsOfUseTypeDesc, request::getTermsOfUseTypeDesc);
        int affected = dbAccessor.executeUpdate(sql, termsOfUseTypeDesc, request.getTermsOfUseTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTermsOfUseText(TermsOfUseIdProto request,
            StreamObserver<GetTermsOfUseTextResponse> responseObserver) {
        validateTermsOfUseIdProto(request);
        String sql = """
                SELECT terms_text FROM terms_of_use WHERE terms_of_use_id=?
                """;
        List<byte[]> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getBytes(1);
        }, request.getTermsOfUseId());

        responseObserver.onNext(result.isEmpty() ? GetTermsOfUseTextResponse.getDefaultInstance()
                : GetTermsOfUseTextResponse.newBuilder().setTermsText(new String(result.get(0))).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setTermsOfUseText(SetTermsOfUseTextRequest request, StreamObserver<CountProto> responseObserver) {
        validateSetTermsOfUseTextRequest(request);
        String sql = """
                UPDATE terms_of_use SET terms_text=? WHERE terms_of_use_id=?
                """;
        String termsText = Helper.extract(request::hasTermsText, request::getTermsText);
        byte[] termsTextBytes = termsText == null ? null : termsText.getBytes();
        int affected = dbAccessor.executeUpdate(sql, termsTextBytes, request.getTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createDependencyRelationship(CreateDependencyRelationshipRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateCreateDependencyRelationshipRequest(request);
        String sql = """
                INSERT INTO terms_of_use_dependency(dependency_terms_of_use_id, dependent_terms_of_use_id)
                VALUES (?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getDependencyTermsOfUseId(),
                request.getDependentTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDependencyTermsOfUse(DependentTermsOfUseIdProto request,
            StreamObserver<GetDependencyTermsOfUseResponse> responseObserver) {
        validateDependentTermsOfUseIdProto(request);
        String sql = """
                SELECT terms_of_use_id, terms_of_use_type_id, title, url, tou.terms_of_use_agreeability_type_id, touat.name as terms_of_use_agreeability_type_name,
                touat.description as terms_of_use_agreeability_type_description
                FROM terms_of_use_dependency d
                INNER JOIN terms_of_use tou ON tou.terms_of_use_id = d.dependency_terms_of_use_id
                INNER JOIN terms_of_use_agreeability_type_lu touat ON touat.terms_of_use_agreeability_type_id = tou.terms_of_use_agreeability_type_id
                WHERE d.dependent_terms_of_use_id = ?
                """;
        List<GetTermsOfUseResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadTermsOfUse(rs),
                request.getDependentTermsOfUseId());
        responseObserver.onNext(GetDependencyTermsOfUseResponse.newBuilder().addAllTermsOfUses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDependentTermsOfUse(DependencyTermsOfUseIdProto request,
            StreamObserver<GetDependentTermsOfUseResponse> responseObserver) {
        validateDependencyTermsOfUseIdProto(request);
        String sql = """
                SELECT terms_of_use_id, terms_of_use_type_id, title, url, tou.terms_of_use_agreeability_type_id, touat.name as terms_of_use_agreeability_type_name,
                touat.description as terms_of_use_agreeability_type_description
                FROM terms_of_use_dependency d
                INNER JOIN terms_of_use tou ON tou.terms_of_use_id = d.dependent_terms_of_use_id
                INNER JOIN terms_of_use_agreeability_type_lu touat ON touat.terms_of_use_agreeability_type_id = tou.terms_of_use_agreeability_type_id
                WHERE d.dependency_terms_of_use_id = ?
                """;
        List<GetTermsOfUseResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadTermsOfUse(rs),
                request.getDependencyTermsOfUseId());
        responseObserver.onNext(GetDependentTermsOfUseResponse.newBuilder().addAllTermsOfUses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteDependencyRelationship(DeleteDependencyRelationshipRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeleteDependencyRelationshipRequest(request);
        String sql = """
                DELETE FROM terms_of_use_dependency WHERE dependency_terms_of_use_id = ? AND dependent_terms_of_use_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getDependencyTermsOfUseId(),
                request.getDependentTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteAllDependencyRelationshipsForDependent(DependentTermsOfUseIdProto request,
            StreamObserver<CountProto> responseObserver) {
        validateDependentTermsOfUseIdProto(request);
        String sql = """
                DELETE FROM terms_of_use_dependency
                WHERE dependent_terms_of_use_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getDependentTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteAllDependencyRelationshipsForDependency(DependencyTermsOfUseIdProto request,
            StreamObserver<CountProto> responseObserver) {
        validateDependencyTermsOfUseIdProto(request);
        String sql = """
                DELETE FROM terms_of_use_dependency
                WHERE dependency_terms_of_use_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getDependencyTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDependencyTermsOfUseIds(GetDependencyTermsOfUseIdsRequest request,
            StreamObserver<GetDependencyTermsOfUseIdsResponse> responseObserver) {
        validateGetDependencyTermsOfUseIdsRequest(request);
        String sql = """
                SELECT DISTINCT dependency_terms_of_use_id
                FROM terms_of_use_dependency
                WHERE dependent_terms_of_use_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getDependentTermsOfUseIdsCount());
        List<Long> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return rs.getLong(1);
        }, request.getDependentTermsOfUseIdsList().toArray());
        responseObserver
                .onNext(GetDependencyTermsOfUseIdsResponse.newBuilder().addAllDependencyTermsOfUseIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createUserTermsOfUse(CreateUserTermsOfUseRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateCreateUserTermsOfUseRequest(request);
        String sql = """
                INSERT INTO user_terms_of_use_xref (user_id,terms_of_use_id)
                VALUES (?,?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getUserId(), request.getTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteUserTermsOfUse(DeleteUserTermsOfUseRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeleteUserTermsOfUseRequest(request);
        String sql = """
                DELETE FROM user_terms_of_use_xref
                WHERE user_id=? AND terms_of_use_id=?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getUserId(), request.getTermsOfUseId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTermsOfUseByUserId(UserIdProto request,
            StreamObserver<GetTermsOfUseByUserIdResponse> responseObserver) {
        validateUserIdProto(request);
        String sql = """
                SELECT tou.terms_of_use_id, terms_of_use_type_id, title, url, tou.terms_of_use_agreeability_type_id, touat.name as terms_of_use_agreeability_type_name,
                touat.description as terms_of_use_agreeability_type_description
                FROM user_terms_of_use_xref
                INNER JOIN terms_of_use tou ON user_terms_of_use_xref.terms_of_use_id = tou.terms_of_use_id
                INNER JOIN terms_of_use_agreeability_type_lu touat ON touat.terms_of_use_agreeability_type_id = tou.terms_of_use_agreeability_type_id
                WHERE user_id=?
                """;
        List<GetTermsOfUseResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> loadTermsOfUse(rs),
                request.getUserId());
        responseObserver.onNext(GetTermsOfUseByUserIdResponse.newBuilder().addAllTermsOfUses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getUsersByTermsOfUseId(TermsOfUseIdProto request,
            StreamObserver<GetUsersByTermsOfUseIdResponse> responseObserver) {
        validateTermsOfUseIdProto(request);
        String sql = """
                SELECT user_id
                FROM user_terms_of_use_xref
                WHERE terms_of_use_id=?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getTermsOfUseId());
        responseObserver.onNext(GetUsersByTermsOfUseIdResponse.newBuilder().addAllUserIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isTermsOfUseExists(TermsOfUseExistsRequest request, StreamObserver<ExistsProto> responseObserver) {
        validateTermsOfUseExistsRequest(request);
        boolean result = checkEntityExists("user_terms_of_use_xref", request.getUserId(), request.getTermsOfUseId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isTermsOfUseBanExists(TermsOfUseExistsRequest request, StreamObserver<ExistsProto> responseObserver) {
        validateTermsOfUseExistsRequest(request);
        boolean result = checkEntityExists("user_terms_of_use_ban_xref", request.getUserId(),
                request.getTermsOfUseId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(result).build());
        responseObserver.onCompleted();
    }

    private GetTermsOfUseResponse loadTermsOfUse(ResultSet rs) throws SQLException {
        GetTermsOfUseResponse.Builder builder = GetTermsOfUseResponse.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, 1, builder::setTermsOfUseId);
        ResultSetHelper.applyResultSetInt(rs, 2, builder::setTermsOfUseTypeId);
        ResultSetHelper.applyResultSetString(rs, 3, builder::setTitle);
        ResultSetHelper.applyResultSetString(rs, 4, builder::setUrl);
        ResultSetHelper.applyResultSetInt(rs, 5, builder::setTermsOfUseAgreeabilityTypeId);
        ResultSetHelper.applyResultSetString(rs, 6, builder::setTermsOfUseAgreeabilityTypeName);
        ResultSetHelper.applyResultSetString(rs, 7, builder::setTermsOfUseAgreeabilityTypeDescription);
        return builder.build();
    }

    private boolean checkEntityExists(String tableName, long userId, long termsOfUseId) {
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM %s WHERE user_id=? AND terms_of_use_id=?) THEN 1 ELSE 0 END FROM DUAL
                """
                .formatted(tableName);
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, userId, termsOfUseId).get(0);
    }

    private void validateCreateProjectRoleTermsOfUseRequest(CreateProjectRoleTermsOfUseRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
        Helper.assertObjectNotNull(request::hasSortOrder, "sort_order");
        Helper.assertObjectNotNull(request::hasGroupInd, "group_ind");
    }

    private void validateDeleteProjectRoleTermsOfUseRequest(DeleteProjectRoleTermsOfUseRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
        Helper.assertObjectNotNull(request::hasGroupInd, "group_ind");
    }

    private void validateGetProjectRoleTermsOfUseRequest(GetProjectRoleTermsOfUseRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasResourceRoleId, "resource_role_id");
    }

    private void validateProjectIdProto(ProjectIdProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateCreateTermsOfUseRequest(CreateTermsOfUseRequest request) {
        Helper.assertObjectNotNull(request::hasTermsOfUseTypeId, "terms_of_use_type_id");
        Helper.assertObjectNotNull(request::hasTitle, "title");
        Helper.assertObjectNotNull(request::hasTermsOfUseAgreeabilityTypeId, "terms_of_use_agreeability_type_id");
    }

    private void validateUpdateTermsOfUseRequest(UpdateTermsOfUseRequest request) {
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
        Helper.assertObjectNotNull(request::hasTermsOfUseTypeId, "terms_of_use_type_id");
        Helper.assertObjectNotNull(request::hasTitle, "title");
        Helper.assertObjectNotNull(request::hasTermsOfUseAgreeabilityTypeId, "terms_of_use_agreeability_type_id");
    }

    private void validateTermsOfUseIdProto(TermsOfUseIdProto request) {
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
    }

    private void validateTermsOfUseTypeIdProto(TermsOfUseTypeIdProto request) {
        Helper.assertObjectNotNull(request::hasTermsOfUseTypeId, "terms_of_use_type_id");
    }

    private void validateTermsOfUseTypeProto(TermsOfUseTypeProto request) {
        Helper.assertObjectNotNull(request::hasTermsOfUseTypeId, "terms_of_use_type_id");
    }

    private void validateSetTermsOfUseTextRequest(SetTermsOfUseTextRequest request) {
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
    }

    private void validateCreateDependencyRelationshipRequest(CreateDependencyRelationshipRequest request) {
        Helper.assertObjectNotNull(request::hasDependentTermsOfUseId, "dependent_terms_of_use_id");
        Helper.assertObjectNotNull(request::hasDependencyTermsOfUseId, "dependency_terms_of_use_id");
    }

    private void validateDependentTermsOfUseIdProto(DependentTermsOfUseIdProto request) {
        Helper.assertObjectNotNull(request::hasDependentTermsOfUseId, "dependent_terms_of_use_id");
    }

    private void validateDependencyTermsOfUseIdProto(DependencyTermsOfUseIdProto request) {
        Helper.assertObjectNotNull(request::hasDependencyTermsOfUseId, "dependency_terms_of_use_id");
    }

    private void validateDeleteDependencyRelationshipRequest(DeleteDependencyRelationshipRequest request) {
        Helper.assertObjectNotNull(request::hasDependentTermsOfUseId, "dependent_terms_of_use_id");
        Helper.assertObjectNotNull(request::hasDependencyTermsOfUseId, "dependency_terms_of_use_id");
    }

    private void validateGetDependencyTermsOfUseIdsRequest(GetDependencyTermsOfUseIdsRequest request) {
        Helper.assertObjectNotEmpty(request::getDependentTermsOfUseIdsCount, "dependent_terms_of_use_ids");
    }

    private void validateCreateUserTermsOfUseRequest(CreateUserTermsOfUseRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
    }

    private void validateDeleteUserTermsOfUseRequest(DeleteUserTermsOfUseRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
    }

    private void validateUserIdProto(UserIdProto request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateTermsOfUseExistsRequest(TermsOfUseExistsRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
        Helper.assertObjectNotNull(request::hasTermsOfUseId, "terms_of_use_id");
    }
}
