package com.topcoder.or.repository;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import com.topcoder.onlinereview.grpc.project.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ProjectService extends ProjectServiceGrpc.ProjectServiceImplBase {
    private final DBAccessor dbAccessor;

    public ProjectService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @PostConstruct
    public void postRun() {

    }

    @Override
    public void getProjects(GetProjectsRequest request, StreamObserver<GetProjectsResponse> responseObserver) {
        validateGetProjectsRequest(request);
        String sql = """
                SELECT project.project_id, status.project_status_id, status.name as status_name, category.project_category_id,
                category.name as category_name, type.project_type_id, type.name as type_name, project.create_user, project.create_date,
                project.modify_user, project.modify_date, category.description, project.tc_direct_project_id, tcdp.name as tc_direct_project_name
                FROM project
                JOIN project_status_lu AS status ON project.project_status_id=status.project_status_id
                JOIN project_category_lu AS category ON project.project_category_id=category.project_category_id
                JOIN project_type_lu AS type ON category.project_type_id=type.project_type_id
                LEFT OUTER JOIN tc_direct_project AS tcdp ON tcdp.project_id=project.tc_direct_project_id
                WHERE project.project_id IN
                """;
        String inSql = " (%s)".formatted(Helper.getInClause(request.getProjectIdsCount()));
        List<ProjectProto> result = dbAccessor.executeQuery(sql.concat(inSql), (rs, _i) -> {
            ProjectProto.Builder builder = ProjectProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setProjectStatusId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setStatusName);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setProjectCategoryId);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setCategoryName);
            ResultSetHelper.applyResultSetLong(rs, 6, builder::setProjectTypeId);
            ResultSetHelper.applyResultSetString(rs, 7, builder::setTypeName);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 9, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 10, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 11, builder::setModifyDate);
            ResultSetHelper.applyResultSetString(rs, 12, builder::setDescription);
            ResultSetHelper.applyResultSetLong(rs, 13, builder::setDirectProjectId);
            ResultSetHelper.applyResultSetString(rs, 14, builder::setTcDirectProjectName);
            return builder.build();
        }, request.getProjectIdsList().toArray());
        responseObserver.onNext(GetProjectsResponse.newBuilder().addAllProjects(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectIdsByDirectId(IdProto request,
            StreamObserver<GetProjectIdsByDirectIdResponse> responseObserver) {
        validateIdProto(request);
        String sql = """
                SELECT DISTINCT project_id FROM project WHERE tc_direct_project_id = ?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getId());
        responseObserver.onNext(GetProjectIdsByDirectIdResponse.newBuilder().addAllProjectIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createProject(CreateProjectRequest request, StreamObserver<CountProto> responseObserver) {
        validateCreateProjectRequest(request);
        String sql = """
                INSERT INTO project (project_id, project_status_id, project_category_id, create_user, create_date, modify_user, modify_date, tc_direct_project_id)
                VALUES (?, ?, ?, ?, CURRENT, ?, CURRENT, ?)
                """;
        final Long directProjectId = Helper.extract(request::hasDirectProjectId, request::getDirectProjectId);
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getProjectStatusId(),
                request.getProjectCategoryId(), request.getCreateUser(), request.getModifyUser(), directProjectId);
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProject(UpdateProjectRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateProjectRequest(request);
        String sql = """
                UPDATE project SET project_status_id=?, project_category_id=?, modify_user=?, modify_date=?, tc_direct_project_id=?
                WHERE project_id=?
                """;
        final Long directProjectId = Helper.extract(request::hasDirectProjectId, request::getDirectProjectId);
        int affected = dbAccessor.executeUpdate(sql, request.getProjectStatusId(), request.getProjectCategoryId(),
                request.getModifyUser(), Helper.convertDate(request.getModifyDate()), directProjectId,
                request.getProjectId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectCreateDate(ProjectIdProto request, StreamObserver<CreateDateProto> responseObserver) {
        validateProjectIdProto(request);
        String sql = """
                SELECT create_date FROM project WHERE project_id = ?
                    """;
        List<CreateDateProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            CreateDateProto.Builder builder = CreateDateProto.newBuilder();
            ResultSetHelper.applyResultSetTimestamp(rs, 1, builder::setCreateDate);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(result.isEmpty() ? CreateDateProto.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectPropertyIdValue(IdProto request,
            StreamObserver<GetProjectPropertyIdValueResponse> responseObserver) {
        validateIdProto(request);
        String sql = """
                SELECT project_info_type_id, value
                FROM project_info
                WHERE project_id = ?
                """;
        List<ProjectPropertyIdValue> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectPropertyIdValue.Builder builder = ProjectPropertyIdValue.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectInfoTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setValue);
            return builder.build();
        }, request.getId());
        GetProjectPropertyIdValueResponse response = GetProjectPropertyIdValueResponse.newBuilder()
                .addAllProjectProperties(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectsProperties(GetProjectsPropertiesRequest request,
            StreamObserver<GetProjectsPropertiesResponse> responseObserver) {
        validateGetProjectsPropertiesResponse(request);
        String sql = """
                SELECT info.project_id, info_type.name, info.value
                FROM project_info AS info
                JOIN project_info_type_lu AS info_type ON info.project_info_type_id=info_type.project_info_type_id
                WHERE info.project_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getProjectIdsCount());
        List<ProjectPropertyProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ProjectPropertyProto.Builder builder = ProjectPropertyProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setValue);
            return builder.build();
        }, request.getProjectIdsList().toArray());
        GetProjectsPropertiesResponse response = GetProjectsPropertiesResponse.newBuilder()
                .addAllProperties(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createProjectProperty(CreateProjectPropertyRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateCreateProjectPropertyRequest(request);
        String sql = """
                INSERT INTO project_info (project_id, project_info_type_id, value, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, CURRENT, ?, CURRENT)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getProjectInfoTypeId(),
                request.getValue(), request.getCreateUser(), request.getModifyUser());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProjectProperty(UpdateProjectPropertyRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateUpdateProjectPropertyRequest(request);
        String sql = """
                UPDATE project_info SET value=?, modify_user=?, modify_date=CURRENT WHERE project_id=? AND project_info_type_id=?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getValue(), request.getModifyUser(),
                request.getProjectId(), request.getProjectInfoTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteProjectProperty(DeleteProjectPropertyRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeleteProjectPropertyRequest(request);
        String sql = """
                DELETE FROM project_info
                WHERE project_id=? AND project_info_type_id IN (%s)
                    """;
        String inSql = Helper.getInClause(request.getProjectInfoTypeIdsCount());
        List<Object> param = new ArrayList<>();
        param.add(request.getProjectId());
        param.addAll(request.getProjectInfoTypeIdsList());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), param.toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void auditProjectInfo(AuditProjectInfoRequest request, StreamObserver<CountProto> responseObserver) {
        validateAuditProjectInfoRequest(request);
        String sql = """
                INSERT INTO project_info_audit (project_id, project_info_type_id, value, audit_action_type_id, action_date, action_user_id)
                VALUES (?, ?, ?, ?, CURRENT, ?)
                """;
        String value = Helper.extract(request::hasValue, request::getValue);
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getProjectInfoTypeId(), value,
                request.getAuditType(), request.getActionUserId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void auditProject(AuditProjectRequest request, StreamObserver<CountProto> responseObserver) {
        validateAuditProjectRequest(request);
        String sql = """
                INSERT INTO project_audit (project_audit_id, project_id, update_reason, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, CURRENT, ?, CURRENT)
                    """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectAuditId(), request.getProjectId(),
                request.getUpdateReason(), request.getCreateUser(), request.getModifyUser());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectFileTypes(ProjectIdProto request,
            StreamObserver<GetProjectFileTypesResponse> responseObserver) {
        validateProjectIdProto(request);
        String sql = """
                SELECT type.file_type_id, type.description, type.sort, type.image_file, type.extension, type.bundled_file
                FROM file_type_lu AS type
                JOIN project_file_type_xref AS xref ON type.file_type_id=xref.file_type_id
                WHERE xref.project_id = ?
                """;
        List<FileTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            FileTypeProto.Builder builder = FileTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setFileTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDescription);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setSort);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setImageFile);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setExtension);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setBundledFile);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(GetProjectFileTypesResponse.newBuilder().addAllFileTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createProjectFileType(CreateProjectFileTypeRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateCreateProjectFileTypeRequest(request);
        String sql = """
                INSERT INTO project_file_type_xref (project_id, file_type_id)
                VALUES (?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getFileTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteProjectFileType(DeleteProjectFileTypeRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateDeleteProjectFileTypeRequest(request);
        String sql = """
                DELETE FROM project_file_type_xref WHERE project_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createFileType(CreateFileTypeRequest request, StreamObserver<CountProto> responseObserver) {
        validateCreateFileTypeRequest(request);
        String sql = """
                INSERT INTO file_type_lu (file_type_id, description, sort, image_file, extension, bundled_file, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getFileTypeId(), request.getDescription(),
                request.getSort(), request.getImageFile(), request.getExtension(), request.getBundledFile(),
                request.getCreateUser(), Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateFileType(UpdateFileTypeRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateFileTypeRequest(request);
        String sql = """
                UPDATE file_type_lu SET description=?, sort=?, image_file=?, extension=?, bundled_file=?, modify_user=?, modify_date=?
                WHERE file_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getDescription(), request.getSort(),
                request.getImageFile(), request.getExtension(), request.getBundledFile(), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getFileTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isProjectExists(IdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateIdProto(request);
        boolean exists = checkEntityExists("project", "project_id", request.getId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(exists).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isFileTypeExists(IdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateIdProto(request);
        boolean exists = checkEntityExists("file_type_lu", "file_type_id", request.getId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(exists).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isPrizeExists(IdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateIdProto(request);
        boolean exists = checkEntityExists("prize", "prize_id", request.getId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(exists).build());
        responseObserver.onCompleted();
    }

    @Override
    public void isStudioSpecExists(IdProto request, StreamObserver<ExistsProto> responseObserver) {
        validateIdProto(request);
        boolean exists = checkEntityExists("project_studio_specification", "project_studio_spec_id", request.getId());
        responseObserver.onNext(ExistsProto.newBuilder().setExists(exists).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectPrizes(ProjectIdProto request, StreamObserver<GetProjectPrizesResponse> responseObserver) {
        validateProjectIdProto(request);
        String sql = """
                SELECT prize.prize_id, prize.place, prize.prize_amount, prize.number_of_submissions, prize_type.prize_type_id, prize_type.prize_type_desc
                FROM prize AS prize
                JOIN prize_type_lu AS prize_type ON prize.prize_type_id=prize_type.prize_type_id
                WHERE prize.project_id = ?
                """;
        List<ProjectPrizeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectPrizeProto.Builder builder = ProjectPrizeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setPrizeId);
            ResultSetHelper.applyResultSetInt(rs, 2, builder::setPlace);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setPrizeAmount);
            ResultSetHelper.applyResultSetInt(rs, 4, builder::setNumberOfSubmissions);
            ResultSetHelper.applyResultSetLong(rs, 5, builder::setPrizeTypeId);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setPrizeTypeDesc);
            return builder.build();
        }, request.getProjectId());
        responseObserver.onNext(GetProjectPrizesResponse.newBuilder().addAllPrizes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createPrize(CreatePrizeRequest request, StreamObserver<CountProto> responseObserver) {
        validateCreatePrizeRequest(request);
        String sql = """
                INSERT INTO prize (prize_id, project_id, place, prize_amount, prize_type_id, number_of_submissions, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getPrizeId(), request.getProjectId(), request.getPlace(),
                request.getPrizeAmount(), request.getPrizeTypeId(), request.getNumberOfSubmissions(),
                request.getCreateUser(), Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePrize(UpdatePrizeRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdatePrizeRequest(request);
        String sql = """
                UPDATE prize SET project_id=?, place=?, prize_amount=?, prize_type_id=?, number_of_submissions=?, modify_user=?, modify_date=?
                WHERE prize_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectId(), request.getPlace(),
                request.getPrizeAmount(), request.getPrizeTypeId(), request.getNumberOfSubmissions(),
                request.getModifyUser(), Helper.convertDate(request.getModifyDate()), request.getPrizeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePrize(DeletePrizeRequest request, StreamObserver<CountProto> responseObserver) {
        validateDeletePrizeRequest(request);
        String sql = """
                DELETE FROM prize WHERE prize_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getPrizeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPrizeProjectIds(GetPrizeProjectIdsRequest request,
            StreamObserver<GetPrizeProjectIdsResponse> responseObserver) {
        validateGetPrizeProjectIdsRequest(request);
        String sql = """
                SELECT project_id FROM prize WHERE prize_id = ?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getPrizeId());
        responseObserver.onNext(GetPrizeProjectIdsResponse.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectStudioSpec(ProjectIdProto request,
            StreamObserver<GetProjectStudioSpecResponse> responseObserver) {
        validateProjectIdProto(request);
        String sql = """
                SELECT spec.project_studio_spec_id, spec.goals, spec.target_audience, spec.branding_guidelines, spec.disliked_design_websites,
                spec.other_instructions, spec.winning_criteria, spec.submitters_locked_between_rounds, spec.round_one_introduction,
                spec.round_two_introduction, spec.colors, spec.fonts, spec.layout_and_size
                FROM project_studio_specification AS spec
                JOIN project AS project ON project.project_studio_spec_id=spec.project_studio_spec_id
                WHERE project.project_id = ?
                """;
        List<GetProjectStudioSpecResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetProjectStudioSpecResponse.Builder builder = GetProjectStudioSpecResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectStudioSpecId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setGoals);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setTargetAudience);
            ResultSetHelper.applyResultSetString(rs, 4, builder::setBrandingGuidelines);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setDislikedDesignWebsites);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setOtherInstructions);
            ResultSetHelper.applyResultSetString(rs, 7, builder::setWinningCriteria);
            ResultSetHelper.applyResultSetBool(rs, 8, builder::setSubmittersLockedBetweenRounds);
            ResultSetHelper.applyResultSetString(rs, 9, builder::setRoundOneIntroduction);
            ResultSetHelper.applyResultSetString(rs, 10, builder::setRoundTwoIntroduction);
            ResultSetHelper.applyResultSetString(rs, 11, builder::setColors);
            ResultSetHelper.applyResultSetString(rs, 12, builder::setFonts);
            ResultSetHelper.applyResultSetString(rs, 13, builder::setLayoutAndSize);
            return builder.build();
        }, request.getProjectId());
        GetProjectStudioSpecResponse response;
        if (result.isEmpty()) {
            response = GetProjectStudioSpecResponse.getDefaultInstance();
        } else {
            response = result.get(0);
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createStudioSpec(CreateStudioSpecRequest request, StreamObserver<CountProto> responseObserver) {
        validateCreateStudioSpecRequest(request);
        String sql = """
                INSERT INTO project_studio_specification (project_studio_spec_id, goals, target_audience, branding_guidelines,
                disliked_design_websites, other_instructions, winning_criteria, submitters_locked_between_rounds, round_one_introduction,
                round_two_introduction, colors, fonts, layout_and_size, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final String goals = Helper.extract(request::hasGoals, request::getGoals);
        final String targetAudience = Helper.extract(request::hasTargetAudience, request::getTargetAudience);
        final String brandingGuidelines = Helper.extract(request::hasBrandingGuidelines,
                request::getBrandingGuidelines);
        final String dislikedDesignWebsites = Helper.extract(request::hasDislikedDesignWebsites,
                request::getDislikedDesignWebsites);
        final String otherInstructions = Helper.extract(request::hasOtherInstructions, request::getOtherInstructions);
        final String winningCriteria = Helper.extract(request::hasWinningCriteria, request::getWinningCriteria);
        final Boolean submittersLockedBetweenRounds = Helper.extract(request::hasSubmittersLockedBetweenRounds,
                request::getSubmittersLockedBetweenRounds);
        final String roundOneIntroduction = Helper.extract(request::hasRoundOneIntroduction,
                request::getRoundOneIntroduction);
        final String roundTwoIntroduction = Helper.extract(request::hasRoundTwoIntroduction,
                request::getRoundTwoIntroduction);
        final String colors = Helper.extract(request::hasColors, request::getColors);
        final String fonts = Helper.extract(request::hasFonts, request::getFonts);
        final String layoutAndSize = Helper.extract(request::hasLayoutAndSize, request::getLayoutAndSize);
        int affected = dbAccessor.executeUpdate(sql, request.getProjectStudioSpecId(), goals, targetAudience,
                brandingGuidelines, dislikedDesignWebsites, otherInstructions, winningCriteria,
                submittersLockedBetweenRounds, roundOneIntroduction, roundTwoIntroduction, colors, fonts, layoutAndSize,
                request.getCreateUser(), Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()));
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateStudioSpec(UpdateStudioSpecRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateStudioSpecRequest(request);
        String sql = """
                UPDATE project_studio_specification SET goals=?, target_audience=?, branding_guidelines=?, disliked_design_websites=?,
                other_instructions=?, winning_criteria=?, submitters_locked_between_rounds=?, round_one_introduction=?,
                round_two_introduction=?, colors=?, fonts=?, layout_and_size=?, modify_user=?, modify_date=?
                WHERE project_studio_spec_id = ?
                """;
        final String goals = Helper.extract(request::hasGoals, request::getGoals);
        final String targetAudience = Helper.extract(request::hasTargetAudience, request::getTargetAudience);
        final String brandingGuidelines = Helper.extract(request::hasBrandingGuidelines,
                request::getBrandingGuidelines);
        final String dislikedDesignWebsites = Helper.extract(request::hasDislikedDesignWebsites,
                request::getDislikedDesignWebsites);
        final String otherInstructions = Helper.extract(request::hasOtherInstructions, request::getOtherInstructions);
        final String winningCriteria = Helper.extract(request::hasWinningCriteria, request::getWinningCriteria);
        final Boolean submittersLockedBetweenRounds = Helper.extract(request::hasSubmittersLockedBetweenRounds,
                request::getSubmittersLockedBetweenRounds);
        final String roundOneIntroduction = Helper.extract(request::hasRoundOneIntroduction,
                request::getRoundOneIntroduction);
        final String roundTwoIntroduction = Helper.extract(request::hasRoundTwoIntroduction,
                request::getRoundTwoIntroduction);
        final String colors = Helper.extract(request::hasColors, request::getColors);
        final String fonts = Helper.extract(request::hasFonts, request::getFonts);
        final String layoutAndSize = Helper.extract(request::hasLayoutAndSize, request::getLayoutAndSize);
        int affected = dbAccessor.executeUpdate(sql, goals, targetAudience, brandingGuidelines, dislikedDesignWebsites,
                otherInstructions, winningCriteria, submittersLockedBetweenRounds, roundOneIntroduction,
                roundTwoIntroduction, colors, fonts, layoutAndSize, request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getProjectStudioSpecId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteStudioSpec(DeleteStudioSpecRequest request, StreamObserver<CountProto> responseObserver) {
        validateDeleteStudioSpecRequest(request);
        String sql = """
                DELETE FROM project_studio_specification WHERE project_studio_spec_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectStudioSpecId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getStudioSpecProjectIds(GetStudioSpecProjectIdsRequest request,
            StreamObserver<GetStudioSpecProjectIdsResponse> responseObserver) {
        validateGetStudioSpecProjectIdsRequest(request);
        String sql = """
                SELECT DISTINCT project_id FROM project WHERE project_studio_spec_id = ?
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, request.getProjectStudioSpecId());
        responseObserver.onNext(GetStudioSpecProjectIdsResponse.newBuilder().addAllProjectIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void removeStudioSpecFromProjects(RemoveStudioSpecFromProjectsRequest request,
            StreamObserver<CountProto> responseObserver) {
        validateRemoveStudioSpecFromProjectsRequest(request);
        String sql = """
                UPDATE project SET project_studio_spec_id = NULL WHERE project_studio_spec_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectStudioSpecId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    private boolean checkEntityExists(String tableName, String columnName, long id) {
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 from %s where %s = ?) THEN 1 ELSE 0 END FROM DUAL
                """.formatted(tableName, columnName);
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, id).get(0);
    }

    private void validateGetProjectsRequest(GetProjectsRequest request) {
        Helper.assertObjectNotEmpty(request::getProjectIdsCount, "project_ids");
    }

    private void validateCreateProjectRequest(CreateProjectRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasProjectStatusId, "project_status_id");
        Helper.assertObjectNotNull(request::hasProjectCategoryId, "project_category_id");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
    }

    private void validateUpdateProjectRequest(UpdateProjectRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasProjectStatusId, "project_status_id");
        Helper.assertObjectNotNull(request::hasProjectCategoryId, "project_category_id");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateProjectIdProto(ProjectIdProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateGetProjectsPropertiesResponse(GetProjectsPropertiesRequest request) {
        Helper.assertObjectNotEmpty(request::getProjectIdsCount, "project_ids");
    }

    private void validateCreateProjectPropertyRequest(CreateProjectPropertyRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasProjectInfoTypeId, "project_info_type_id");
        Helper.assertObjectNotNull(request::hasValue, "value");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
    }

    private void validateUpdateProjectPropertyRequest(UpdateProjectPropertyRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasProjectInfoTypeId, "project_info_type_id");
        Helper.assertObjectNotNull(request::hasValue, "value");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
    }

    private void validateDeleteProjectPropertyRequest(DeleteProjectPropertyRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotEmpty(request::getProjectInfoTypeIdsCount, "project_info_type_ids");
    }

    private void validateAuditProjectInfoRequest(AuditProjectInfoRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasProjectInfoTypeId, "project_info_type_id");
        Helper.assertObjectNotNull(request::hasAuditType, "audit_type");
        Helper.assertObjectNotNull(request::hasActionUserId, "action_user_id");
    }

    private void validateAuditProjectRequest(AuditProjectRequest request) {
        Helper.assertObjectNotNull(request::hasProjectAuditId, "project_audit_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasUpdateReason, "update_reason");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
    }

    private void validateCreateProjectFileTypeRequest(CreateProjectFileTypeRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasFileTypeId, "file_type_id");
    }

    private void validateDeleteProjectFileTypeRequest(DeleteProjectFileTypeRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateCreateFileTypeRequest(CreateFileTypeRequest request) {
        Helper.assertObjectNotNull(request::hasFileTypeId, "file_type_id");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasImageFile, "image_file");
        Helper.assertObjectNotNull(request::hasExtension, "extension");
        Helper.assertObjectNotNull(request::hasBundledFile, "bundled_file");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateFileTypeRequest(UpdateFileTypeRequest request) {
        Helper.assertObjectNotNull(request::hasFileTypeId, "file_type_id");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasImageFile, "image_file");
        Helper.assertObjectNotNull(request::hasExtension, "extension");
        Helper.assertObjectNotNull(request::hasBundledFile, "bundled_file");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateIdProto(IdProto request) {
        Helper.assertObjectNotNull(request::hasId, "id");
    }

    private void validateCreatePrizeRequest(CreatePrizeRequest request) {
        Helper.assertObjectNotNull(request::hasPrizeId, "prize_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasPlace, "place");
        Helper.assertObjectNotNull(request::hasPrizeAmount, "prize_amount");
        Helper.assertObjectNotNull(request::hasPrizeTypeId, "prize_type_id");
        Helper.assertObjectNotNull(request::hasNumberOfSubmissions, "number_of_submissions");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdatePrizeRequest(UpdatePrizeRequest request) {
        Helper.assertObjectNotNull(request::hasPrizeId, "prize_id");
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasPlace, "place");
        Helper.assertObjectNotNull(request::hasPrizeAmount, "prize_amount");
        Helper.assertObjectNotNull(request::hasPrizeTypeId, "prize_type_id");
        Helper.assertObjectNotNull(request::hasNumberOfSubmissions, "number_of_submissions");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateDeletePrizeRequest(DeletePrizeRequest request) {
        Helper.assertObjectNotNull(request::hasPrizeId, "prize_id");
    }

    private void validateGetPrizeProjectIdsRequest(GetPrizeProjectIdsRequest request) {
        Helper.assertObjectNotNull(request::hasPrizeId, "prize_id");
    }

    private void validateCreateStudioSpecRequest(CreateStudioSpecRequest request) {
        Helper.assertObjectNotNull(request::hasProjectStudioSpecId, "project_studio_spec_id");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateStudioSpecRequest(UpdateStudioSpecRequest request) {
        Helper.assertObjectNotNull(request::hasProjectStudioSpecId, "project_studio_spec_id");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateDeleteStudioSpecRequest(DeleteStudioSpecRequest request) {
        Helper.assertObjectNotNull(request::hasProjectStudioSpecId, "project_studio_spec_id");
    }

    private void validateGetStudioSpecProjectIdsRequest(GetStudioSpecProjectIdsRequest request) {
        Helper.assertObjectNotNull(request::hasProjectStudioSpecId, "project_studio_spec_id");
    }

    private void validateRemoveStudioSpecFromProjectsRequest(RemoveStudioSpecFromProjectsRequest request) {
        Helper.assertObjectNotNull(request::hasProjectStudioSpecId, "project_studio_spec_id");
    }
}