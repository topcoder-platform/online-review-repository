package com.topcoder.or.repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.topcoder.onlinereview.component.id.DBHelper;
import com.topcoder.onlinereview.component.id.IDGenerator;
import com.topcoder.onlinereview.grpc.project.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ProjectService extends ProjectServiceGrpc.ProjectServiceImplBase {
    private final DBAccessor dbAccessor;
    private final DBHelper dbHelper;

    private IDGenerator projectIdGenerator;
    private IDGenerator projectAuditIdGenerator;
    private IDGenerator fileTypeIdGenerator;
    private IDGenerator prizeIdGenerator;
    private IDGenerator studioSpecIdGenerator;

    private String projectIdSeqName = "project_id_seq";
    private String projectAuditIdSeqName = "project_audit_id_seq";
    private String fileTypeIdSeqName = "file_type_id_seq";
    private String prizeIdSeqName = "prize_id_seq";
    private String studioSpecIdSeqName = "studio_spec_id_seq";

    private static final int AUDIT_CREATE_TYPE = 1;
    private static final int AUDIT_DELETE_TYPE = 2;
    private static final int AUDIT_UPDATE_TYPE = 3;

    public ProjectService(DBAccessor dbAccessor, DBHelper dbHelper) {
        this.dbAccessor = dbAccessor;
        this.dbHelper = dbHelper;
    }

    @PostConstruct
    public void postRun() {
        projectIdGenerator = new IDGenerator(projectIdSeqName, dbHelper);
        projectAuditIdGenerator = new IDGenerator(projectAuditIdSeqName, dbHelper);
        fileTypeIdGenerator = new IDGenerator(fileTypeIdSeqName, dbHelper);
        prizeIdGenerator = new IDGenerator(prizeIdSeqName, dbHelper);
        studioSpecIdGenerator = new IDGenerator(studioSpecIdSeqName, dbHelper);
    }

    @Override
    public void createProject(CreateProjectRequest request, StreamObserver<ProjectProto> responseObserver) {
        Map<String, Long> propertyTypeNameIdMap = new HashMap<>();
        if (request.getProject().getPropertiesCount() > 0) {
            propertyTypeNameIdMap = makePropertyNamePropertyIdMap(getAllProjectPropertyTypes());
        }
        validateCreateProjectRequest(request, propertyTypeNameIdMap);
        ProjectProto.Builder pBuilder = createProject(request.getProject(), request.getOperator());
        if (request.getProject().getPropertiesCount() > 0) {
            createProjectProperties(pBuilder.getId(), request.getOperator(), request.getProject().getPropertiesList(),
                    propertyTypeNameIdMap);
        }
        if (request.getProject().getFileTypesCount() > 0) {
            pBuilder.addAllFileTypes(
                    createProjectFileTypes(pBuilder.getId(), request.getProject().getFileTypesList(),
                            request.getOperator()));
        }
        if (request.getProject().getPrizesCount() > 0) {
            pBuilder.clearPrizes();
            pBuilder.addAllPrizes(
                    createProjectPrizes(pBuilder.getId(), request.getProject().getPrizesList(), request.getOperator()));
        }
        if (request.getProject().hasProjectStudioSpec()) {
            pBuilder.setProjectStudioSpec(createProjectStudioSpec(pBuilder.getId(),
                    request.getProject().getProjectStudioSpec(), request.getOperator()));
        }
        responseObserver.onNext(pBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateProject(UpdateProjectRequest request, StreamObserver<ProjectProto> responseObserver) {
        Map<String, Long> propertyTypeNameIdMap = new HashMap<>();
        if (request.getProject().getPropertiesCount() > 0) {
            propertyTypeNameIdMap = makePropertyNamePropertyIdMap(getAllProjectPropertyTypes());
        }
        validateUpdateProjectRequest(request, propertyTypeNameIdMap);
        ProjectProto.Builder pBuilder = updateProject(request.getProject(), request.getReason(),
                request.getOperator());
        if (request.getProject().getPropertiesCount() > 0) {
            updateProjectProperties(pBuilder.getId(), request.getOperator(), request.getProject().getPropertiesList(),
                    propertyTypeNameIdMap);
        }
        if (request.getProject().getFileTypesCount() > 0) {
            pBuilder.addAllFileTypes(
                    updateProjectFileTypes(pBuilder.getId(), request.getProject().getFileTypesList(),
                            request.getOperator()));
        }
        if (request.getProject().getPrizesCount() > 0) {
            pBuilder.addAllPrizes(
                    updateProjectPrizes(pBuilder.getId(), request.getProject().getPrizesList(), request.getOperator()));
        }
        if (request.getProject().hasProjectStudioSpec()) {
            pBuilder.setProjectStudioSpec(updateProjectStudioSpec(pBuilder.getId(),
                    request.getProject().getProjectStudioSpec(), request.getOperator()));
        }
        responseObserver.onNext(pBuilder.build());
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
    public void getProjectFileTypes(ProjectIdProto request,
            StreamObserver<GetProjectFileTypesResponse> responseObserver) {
        validateProjectIdProto(request);
        List<FileTypeProto> result = getProjectFileTypes(request.getProjectId());
        responseObserver.onNext(GetProjectFileTypesResponse.newBuilder().addAllFileTypes(result).build());
        responseObserver.onCompleted();
    }

    private List<FileTypeProto> getProjectFileTypes(long projectId) {
        String sql = """
                SELECT type.file_type_id, type.description, type.sort, type.image_file, type.extension, type.bundled_file
                FROM file_type_lu AS type
                JOIN project_file_type_xref AS xref ON type.file_type_id=xref.file_type_id
                WHERE xref.project_id = ?
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            FileTypeProto.Builder builder = FileTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDescription);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setSort);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setImageFile);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setExtension);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setBundledFile);
            return builder.build();
        }, projectId);
    }

    @Override
    public void updateProjectFileTypes(UpdateProjectFileTypesRequest request,
            StreamObserver<UpdateProjectFileTypesResponse> responseObserver) {
        validateUpdateProjectFileTypesRequest(request);
        List<FileTypeProto> fileTypes = updateProjectFileTypes(request.getProjectId(), request.getFileTypesList(),
                request.getOperator());
        auditProject(request.getProjectId(), "Updates the project file types", request.getOperator());
        responseObserver.onNext(UpdateProjectFileTypesResponse.newBuilder().addAllFileTypes(fileTypes).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectPrizes(ProjectIdProto request, StreamObserver<GetProjectPrizesResponse> responseObserver) {
        validateProjectIdProto(request);

        List<PrizeProto> result = getProjectPrizes(request.getProjectId());
        responseObserver.onNext(GetProjectPrizesResponse.newBuilder().addAllPrizes(result).build());
        responseObserver.onCompleted();
    }

    private List<PrizeProto> getProjectPrizes(long projectId) {
        String sql = """
                SELECT prize.prize_id, prize.place, prize.prize_amount, prize.number_of_submissions, prize_type.prize_type_id, prize_type.prize_type_desc
                FROM prize AS prize
                JOIN prize_type_lu AS prize_type ON prize.prize_type_id=prize_type.prize_type_id
                WHERE prize.project_id = ?
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            PrizeProto.Builder builder = PrizeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetInt(rs, 2, builder::setPlace);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setPrizeAmount);
            ResultSetHelper.applyResultSetInt(rs, 4, builder::setNumberOfSubmissions);
            PrizeTypeProto.Builder ptBuilder = PrizeTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 5, ptBuilder::setId);
            ResultSetHelper.applyResultSetString(rs, 6, ptBuilder::setDescription);
            if (ptBuilder.hasId()) {
                builder.setPrizeType(ptBuilder.build());
            }
            return builder.build();
        }, projectId);
    }

    @Override
    public void updateProjectPrizes(UpdateProjectPrizesRequest request,
            StreamObserver<UpdateProjectPrizesResponse> responseObserver) {
        validateUpdateProjectPrizesRequest(request);
        List<PrizeProto> prizes = updateProjectPrizes(request.getProjectId(), request.getPrizesList(),
                request.getOperator());
        auditProject(request.getProjectId(), "Updates the project prizes", request.getOperator());
        responseObserver.onNext(UpdateProjectPrizesResponse.newBuilder().addAllPrizes(prizes).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createFileType(CreateFileTypeRequest request, StreamObserver<FileTypeProto> responseObserver) {
        validateCreateFileTypeRequest(request);
        FileTypeProto fileType = createFileType(request.getFileType(), request.getOperator());
        responseObserver.onNext(fileType);
        responseObserver.onCompleted();
    }

    @Override
    public void updateFileType(UpdateFileTypeRequest request, StreamObserver<FileTypeProto> responseObserver) {
        validateUpdateFileTypeRequest(request);
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                UPDATE file_type_lu SET description=?, sort=?, image_file=?, extension=?, bundled_file=?, modify_user=?, modify_date=?
                WHERE file_type_id = ?
                """;
        FileTypeProto fileType = request.getFileType();
        dbAccessor.executeUpdate(sql, fileType.getDescription(), fileType.getSort(),
                fileType.getImageFile(), fileType.getExtension(), fileType.getBundledFile(), request.getOperator(),
                now, fileType.getId());
        fileType = FileTypeProto.newBuilder(fileType).setModifyUser(request.getOperator()).setModifyDate(nowTs)
                .build();
        responseObserver.onNext(fileType);
        responseObserver.onCompleted();
    }

    @Override
    public void getAllFileTypes(Empty request, StreamObserver<GetAllFileTypesResponse> responseObserver) {
        String sql = """
                SELECT file_type_id, description, sort, image_file, extension, bundled_file FROM file_type_lu
                """;
        List<FileTypeProto> fileTypes = dbAccessor.executeQuery(sql, (rs, _id) -> {
            FileTypeProto.Builder builder = FileTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDescription);
            ResultSetHelper.applyResultSetInt(rs, 3, builder::setSort);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setImageFile);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setExtension);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setBundledFile);
            return builder.build();
        });
        responseObserver.onNext(GetAllFileTypesResponse.newBuilder().addAllFileTypes(fileTypes).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllPrizeTypes(Empty request, StreamObserver<GetAllPrizeTypesResponse> responseObserver) {
        String sql = """
                SELECT prize_type_id, prize_type_desc FROM prize_type_lu
                """;
        List<PrizeTypeProto> prizeTypes = dbAccessor.executeQuery(sql, (rs, _id) -> {
            PrizeTypeProto.Builder builder = PrizeTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDescription);
            return builder.build();
        });
        responseObserver.onNext(GetAllPrizeTypesResponse.newBuilder().addAllPrizeTypes(prizeTypes).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteFileType(DeleteFileTypeRequest request, StreamObserver<CountProto> responseObserver) {
        validateDeleteFileTypeRequest(request);
        List<Long> projectIds = getProjectIdsByFileType(request.getFileTypeId());
        auditProjects(projectIds, "Removes the project file type", request.getOperator());
        deleteProjectFileTypesByTypeId(request.getFileTypeId());
        int affected = deleteFileType(request.getFileTypeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createPrize(CreatePrizeRequest request, StreamObserver<PrizeProto> responseObserver) {
        validateCreatePrizeRequest(request);
        PrizeProto prize = createPrize(request.getPrize().getProjectId(), request.getPrize(), request.getOperator());
        responseObserver.onNext(prize);
        responseObserver.onCompleted();
    }

    @Override
    public void updatePrize(UpdatePrizeRequest request, StreamObserver<PrizeProto> responseObserver) {
        validateUpdatePrizeRequest(request);
        PrizeProto prize = updatePrize(request.getPrize().getId(), request.getPrize(), request.getOperator());
        responseObserver.onNext(prize);
        responseObserver.onCompleted();
    }

    @Override
    public void deletePrize(DeletePrizeRequest request, StreamObserver<CountProto> responseObserver) {
        validateDeletePrizeRequest(request);
        List<Long> projectIds = getProjectIdsByPrizeId(request.getPrizeId());
        auditProjects(projectIds, "Removes the project prize", request.getOperator());
        String sql = """
                DELETE FROM prize WHERE prize_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getPrizeId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createStudioSpec(CreateStudioSpecRequest request,
            StreamObserver<ProjectStudioSpecProto> responseObserver) {
        validateCreateStudioSpecRequest(request);
        ProjectStudioSpecProto spec = createStudioSpec(request.getProjectStudioSpec(), request.getOperator());
        responseObserver.onNext(spec);
        responseObserver.onCompleted();
    }

    @Override
    public void updateStudioSpec(UpdateStudioSpecRequest request,
            StreamObserver<ProjectStudioSpecProto> responseObserver) {
        validateUpdateStudioSpecRequest(request);
        ProjectStudioSpecProto spec = updateStudioSpec(request.getProjectStudioSpec(), request.getOperator());
        responseObserver.onNext(spec);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteStudioSpec(DeleteStudioSpecRequest request, StreamObserver<CountProto> responseObserver) {
        validateDeleteStudioSpecRequest(request);
        List<Long> projectIds = getProjectIdsByStudioSpecId(request.getProjectStudioSpecId());
        auditProjects(projectIds, "Removes the project studio specification", request.getOperator());
        removeStudioSpecFromProjects(request.getProjectStudioSpecId());
        String sql = """
                DELETE FROM project_studio_specification WHERE project_studio_spec_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getProjectStudioSpecId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjectStudioSpec(ProjectIdProto request,
            StreamObserver<GetProjectStudioSpecResponse> responseObserver) {
        validateProjectIdProto(request);
        responseObserver.onNext(getProjectStudioSpec(request.getProjectId()));
        responseObserver.onCompleted();
    }

    private GetProjectStudioSpecResponse getProjectStudioSpec(long projectId) {
        String sql = """
                SELECT spec.project_studio_spec_id, spec.goals, spec.target_audience, spec.branding_guidelines, spec.disliked_design_websites,
                spec.other_instructions, spec.winning_criteria, spec.submitters_locked_between_rounds, spec.round_one_introduction,
                spec.round_two_introduction, spec.colors, spec.fonts, spec.layout_and_size
                FROM project_studio_specification AS spec
                JOIN project AS project ON project.project_studio_spec_id=spec.project_studio_spec_id
                WHERE project.project_id = ?
                """;
        List<ProjectStudioSpecProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectStudioSpecProto.Builder builder = ProjectStudioSpecProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
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
        }, projectId);
        if (result.isEmpty()) {
            return GetProjectStudioSpecResponse.getDefaultInstance();
        } else {
            return GetProjectStudioSpecResponse.newBuilder().setProjectStudioSpec(result.get(0)).build();
        }
    }

    @Override
    public void updateProjectStudioSpec(UpdateProjectStudioSpecRequest request,
            StreamObserver<ProjectStudioSpecProto> responseObserver) {
        validateUpdateProjectStudioSpecRequest(request);
        ProjectStudioSpecProto spec = updateProjectStudioSpec(request.getProjectId(), request.getProjectStudioSpec(),
                request.getOperator());
        responseObserver.onNext(spec);
        responseObserver.onCompleted();
    }

    @Override
    public void getAllProjectPropertyTypes(Empty request,
            StreamObserver<GetAllProjectPropertyTypesResponse> responseObserver) {
        List<ProjectPropertyTypeProto> result = getAllProjectPropertyTypes();
        responseObserver.onNext(GetAllProjectPropertyTypesResponse.newBuilder().addAllPropertyTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllProjectTypes(Empty request, StreamObserver<GetAllProjectTypesResponse> responseObserver) {
        String sql = """
                SELECT project_type_id, name, description, is_generic FROM project_type_lu
                """;
        List<ProjectTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectTypeProto.Builder builder = ProjectTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setDescription);
            ResultSetHelper.applyResultSetBool(rs, 4, builder::setGeneric);
            return builder.build();
        });
        responseObserver.onNext(GetAllProjectTypesResponse.newBuilder().addAllProjectTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllProjectCategories(Empty request,
            StreamObserver<GetAllProjectCategoriesResponse> responseObserver) {
        String sql = """
                SELECT category.project_category_id, category.name as category_name,category.description as category_description,
                type.project_type_id, type.name as type_name, type.description as type_description, type.is_generic
                FROM project_category_lu AS category
                JOIN project_type_lu AS type ON category.project_type_id = type.project_type_id
                """;
        List<ProjectCategoryProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectCategoryProto.Builder builder = ProjectCategoryProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setDescription);
            ProjectTypeProto.Builder tBuilder = ProjectTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 4, tBuilder::setId);
            ResultSetHelper.applyResultSetString(rs, 5, tBuilder::setName);
            ResultSetHelper.applyResultSetString(rs, 6, tBuilder::setDescription);
            ResultSetHelper.applyResultSetBool(rs, 7, tBuilder::setGeneric);
            builder.setProjectType(tBuilder.build());
            return builder.build();
        });
        responseObserver.onNext(GetAllProjectCategoriesResponse.newBuilder().addAllProjectCategories(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllProjectStatuses(Empty request, StreamObserver<GetAllProjectStatusesResponse> responseObserver) {
        String sql = """
                SELECT project_status_id, name, description FROM project_status_lu
                """;
        List<ProjectStatusProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectStatusProto.Builder builder = ProjectStatusProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setDescription);
            return builder.build();
        });
        responseObserver.onNext(GetAllProjectStatusesResponse.newBuilder().addAllProjectStatuses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProjects(GetProjectsRequest request, StreamObserver<GetProjectsResponse> responseObserver) {
        validateGetProjectsRequest(request);

        List<ProjectProto.Builder> projects = getProjects(request.getProjectIdsList());
        List<ProjectPropertyProto> properties = getProjectsProperties(request.getProjectIdsList());
        Map<Long, List<ProjectPropertyProto>> propertyListMap = new HashMap<>();
        for (ProjectPropertyProto property : properties) {
            if (propertyListMap.containsKey(property.getProjectId())) {
                propertyListMap.get(property.getProjectId()).add(property);
            } else {
                propertyListMap.put(property.getProjectId(), new ArrayList<>() {
                    {
                        add(property);
                    }
                });
            }
        }
        List<ProjectProto> results = new ArrayList<>(projects.size());
        for (ProjectProto.Builder p : projects) {
            p.addAllFileTypes(getProjectFileTypes(p.getId()));
            p.addAllPrizes(getProjectPrizes(p.getId()));
            GetProjectStudioSpecResponse spec = getProjectStudioSpec(p.getId());
            if (spec.hasProjectStudioSpec()) {
                p.setProjectStudioSpec(spec.getProjectStudioSpec());
            }
            if (propertyListMap.containsKey(p.getId())) {
                p.addAllProperties(propertyListMap.get(p.getId()));
            }
            results.add(p.build());
        }
        responseObserver.onNext(GetProjectsResponse.newBuilder().addAllProjects(results).build());
        responseObserver.onCompleted();
    }

    private List<ProjectProto.Builder> getProjects(List<Long> projectIds) {
        String sql = """
                SELECT project.project_id, status.project_status_id, status.name as status_name, category.project_category_id,
                category.name as category_name, category.description, type.project_type_id, type.name as type_name, project.create_user, project.create_date,
                project.modify_user, project.modify_date, project.tc_direct_project_id, tcdp.name as tc_direct_project_name
                FROM project
                JOIN project_status_lu AS status ON project.project_status_id=status.project_status_id
                JOIN project_category_lu AS category ON project.project_category_id=category.project_category_id
                JOIN project_type_lu AS type ON category.project_type_id=type.project_type_id
                LEFT OUTER JOIN tc_direct_project AS tcdp ON tcdp.project_id=project.tc_direct_project_id
                WHERE project.project_id IN
                """;
        String inSql = " (%s)".formatted(Helper.getInClause(projectIds.size()));
        return dbAccessor.executeQuery(sql.concat(inSql), (rs, _i) -> {
            ProjectProto.Builder builder = ProjectProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ProjectStatusProto.Builder psBuilder = ProjectStatusProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 2, psBuilder::setId);
            ResultSetHelper.applyResultSetString(rs, 3, psBuilder::setName);
            builder.setProjectStatus(psBuilder.build());
            ProjectCategoryProto.Builder pcBuilder = ProjectCategoryProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 4, pcBuilder::setId);
            ResultSetHelper.applyResultSetString(rs, 5, pcBuilder::setName);
            ResultSetHelper.applyResultSetString(rs, 6, pcBuilder::setDescription);
            ProjectTypeProto.Builder ptBuilder = ProjectTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 7, ptBuilder::setId);
            ResultSetHelper.applyResultSetString(rs, 8, ptBuilder::setName);
            pcBuilder.setProjectType(ptBuilder.build());
            builder.setProjectCategory(pcBuilder.build());
            ResultSetHelper.applyResultSetString(rs, 9, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 10, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 11, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 12, builder::setModifyDate);
            ResultSetHelper.applyResultSetLong(rs, 13, builder::setDirectProjectId);
            ResultSetHelper.applyResultSetString(rs, 14, builder::setTcDirectProjectName);
            return builder;
        }, projectIds.toArray());
    }

    private List<ProjectPropertyProto> getProjectsProperties(List<Long> projectIds) {
        String sql = """
                SELECT info.project_id, info_type.name, info.value
                FROM project_info AS info
                JOIN project_info_type_lu AS info_type ON info.project_info_type_id=info_type.project_info_type_id
                WHERE info.project_id IN (%s)
                """;
        String inSql = Helper.getInClause(projectIds.size());
        return dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ProjectPropertyProto.Builder builder = ProjectPropertyProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setProjectId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setValue);
            return builder.build();
        }, projectIds.toArray());
    }

    private ProjectProto.Builder createProject(ProjectProto project, String operator) {
        Long newId = projectIdGenerator.getNextID();
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                INSERT INTO project (project_id, project_status_id, project_category_id, create_user, create_date, modify_user, modify_date, tc_direct_project_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final Long directProjectId = Helper.extract(project::hasDirectProjectId, project::getDirectProjectId);
        dbAccessor.executeUpdate(sql, newId, project.getProjectStatus().getId(), project.getProjectCategory().getId(),
                operator, now, operator, now, directProjectId);
        return ProjectProto.newBuilder(project).setId(newId).setCreateUser(operator).setCreateDate(nowTs)
                .setModifyUser(operator).setModifyDate(nowTs);
    }

    private ProjectProto.Builder updateProject(ProjectProto project, String reason, String operator) {
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                UPDATE project SET project_status_id=?, project_category_id=?, modify_user=?, modify_date=?, tc_direct_project_id=?
                WHERE project_id=?
                """;
        final Long directProjectId = Helper.extract(project::hasDirectProjectId, project::getDirectProjectId);
        dbAccessor.executeUpdate(sql, project.getProjectStatus().getId(), project.getProjectCategory().getId(),
                operator, now, directProjectId, project.getId());
        auditProject(project.getId(), reason, operator);
        return ProjectProto.newBuilder(project).setModifyUser(operator).setModifyDate(nowTs);
    }

    private void createProjectProperties(long projectId, String operator, List<ProjectPropertyProto> properties,
            Map<String, Long> nameIdMap) {
        if (properties.isEmpty()) {
            return;
        }
        for (ProjectPropertyProto property : properties) {
            createProjectProperty(projectId, operator, nameIdMap.get(property.getName()), property.getValue());
        }
    }

    private int createProjectProperty(long projectId, String operator, long propertyTypeId, String value) {
        String sql = """
                INSERT INTO project_info (project_id, project_info_type_id, value, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, CURRENT, ?, CURRENT)
                """;
        int affected = dbAccessor.executeUpdate(sql, projectId, propertyTypeId, value, operator, operator);
        auditProjectInfo(projectId, AUDIT_CREATE_TYPE, propertyTypeId, value, operator);
        return affected;
    }

    private int updateProjectProperty(long projectId, long propertyTypeId, String value, String operator) {
        String sql = """
                UPDATE project_info SET value=?, modify_user=?, modify_date=CURRENT WHERE project_id=? AND project_info_type_id=?
                """;
        int affected = dbAccessor.executeUpdate(sql, value, operator, projectId, propertyTypeId);
        auditProjectInfo(projectId, AUDIT_UPDATE_TYPE, propertyTypeId, value, operator);
        return affected;
    }

    private int deleteProjectProperties(long projectId, List<Long> propertyIds, String operator) {
        if (propertyIds == null || propertyIds.isEmpty()) {
            return 0;
        }
        String sql = """
                DELETE FROM project_info
                WHERE project_id=? AND project_info_type_id IN (%s)
                    """;
        String inSql = Helper.getInClause(propertyIds.size());
        List<Object> param = new ArrayList<>();
        param.add(projectId);
        param.addAll(propertyIds);
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), param.toArray());
        for (Long id : propertyIds) {
            auditProjectInfo(projectId, AUDIT_DELETE_TYPE, id, null, operator);
        }
        return affected;
    }

    private void updateProjectProperties(long projectId, String operator, List<ProjectPropertyProto> properties,
            Map<String, Long> nameIdMap) {
        Map<Long, String> existent = makePropertyIdPropertyValueMap(getProjectPropertyIdValues(projectId));
        for (ProjectPropertyProto property : properties) {
            long propertyId = nameIdMap.get(property.getName());
            if (existent.containsKey(propertyId)) {
                if (!existent.get(propertyId).equals(property.getValue())) {
                    updateProjectProperty(projectId, propertyId, property.getValue(), operator);
                }
                existent.remove(propertyId);
            } else {
                createProjectProperty(projectId, operator, propertyId, property.getValue());
            }
        }
        if (!existent.isEmpty()) {
            deleteProjectProperties(projectId, new ArrayList<>(existent.keySet()), operator);
        }
    }

    private List<ProjectPropertyProto> getProjectPropertyIdValues(long projectId) {
        String sql = """
                SELECT project_info_type_id, value
                FROM project_info
                WHERE project_id = ?
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectPropertyProto.Builder builder = ProjectPropertyProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setValue);
            return builder.build();
        }, projectId);
    }

    private List<FileTypeProto> createProjectFileTypes(long projectId, List<FileTypeProto> fileTypes,
            String operator) {
        List<FileTypeProto> newFileTypes = new ArrayList<>();
        for (FileTypeProto fileType : fileTypes) {
            long id = fileType.getId();
            if (id == 0) {
                FileTypeProto created = createFileType(fileType, operator);
                id = created.getId();
                newFileTypes.add(created);
            }
            createProjectFileType(projectId, id);
        }
        return newFileTypes;
    }

    private List<FileTypeProto> updateProjectFileTypes(long projectId, List<FileTypeProto> fileTypes,
            String operator) {
        deleteProjectFileTypes(projectId);
        return createProjectFileTypes(projectId, fileTypes, operator);
    }

    private int deleteProjectFileTypes(long projectId) {
        String sql = """
                DELETE FROM project_file_type_xref WHERE project_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, projectId);
        return affected;
    }

    private int deleteProjectFileTypesByTypeId(long fileTypeId) {
        String sql = """
                DELETE FROM project_file_type_xref WHERE file_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, fileTypeId);
        return affected;
    }

    private int deleteFileType(long fileTypeId) {
        String sql = """
                DELETE FROM file_type_lu WHERE file_type_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, fileTypeId);
        return affected;
    }

    private FileTypeProto createFileType(FileTypeProto fileType, String operator) {
        Long newId = fileTypeIdGenerator.getNextID();
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                INSERT INTO file_type_lu (file_type_id, description, sort, image_file, extension, bundled_file, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, newId, fileType.getDescription(), fileType.getSort(), fileType.getImageFile(),
                fileType.getExtension(), fileType.getBundledFile(), operator, now, operator, now);
        return FileTypeProto.newBuilder(fileType).setId(newId).setCreateUser(operator).setCreateDate(nowTs)
                .setModifyUser(operator).setModifyDate(nowTs).build();
    }

    private int createProjectFileType(long projectId, long fileTypeId) {
        String sql = """
                INSERT INTO project_file_type_xref (project_id, file_type_id)
                VALUES (?, ?)
                """;
        return dbAccessor.executeUpdate(sql, projectId, fileTypeId);
    }

    private List<PrizeProto> updateProjectPrizes(long projectId, List<PrizeProto> prizes, String operator) {
        List<PrizeProto> newPrizes = new ArrayList<>();
        for (PrizeProto prize : prizes) {
            if (prize.getId() == 0) {
                newPrizes.add(createPrize(projectId, prize, operator));
            } else {
                newPrizes.add(updatePrize(projectId, prize, operator));
            }

        }
        return newPrizes;
    }

    private List<PrizeProto> createProjectPrizes(long projectId, List<PrizeProto> prizes, String operator) {
        List<PrizeProto> newPrizes = new ArrayList<>();
        for (PrizeProto prize : prizes) {
            newPrizes.add(createPrize(projectId, prize, operator));
        }
        return newPrizes;
    }

    private PrizeProto createPrize(long projectId, PrizeProto prize, String operator) {
        Long newId = prizeIdGenerator.getNextID();
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                INSERT INTO prize (prize_id, project_id, place, prize_amount, prize_type_id, number_of_submissions, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, newId, projectId, prize.getPlace(), prize.getPrizeAmount(),
                prize.getPrizeType().getId(), prize.getNumberOfSubmissions(), operator, now, operator, now);
        return PrizeProto.newBuilder(prize).setId(newId).setProjectId(projectId).setCreateUser(operator)
                .setCreateDate(nowTs).setModifyUser(operator).setModifyDate(nowTs).build();
    }

    private PrizeProto updatePrize(long projectId, PrizeProto prize, String operator) {
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                UPDATE prize SET project_id=?, place=?, prize_amount=?, prize_type_id=?, number_of_submissions=?, modify_user=?, modify_date=?
                WHERE prize_id = ?
                """;
        dbAccessor.executeUpdate(sql, projectId, prize.getPlace(), prize.getPrizeAmount(),
                prize.getPrizeType().getId(), prize.getNumberOfSubmissions(), operator, now, prize.getId());
        return PrizeProto.newBuilder(prize).setProjectId(projectId).setModifyUser(operator).setModifyDate(nowTs)
                .build();
    }

    private ProjectStudioSpecProto createProjectStudioSpec(long projectId, ProjectStudioSpecProto studioSpec,
            String operator) {
        ProjectStudioSpecProto created = createStudioSpec(studioSpec, operator);
        String sql = """
                UPDATE project
                SET project_studio_spec_id = ?
                WHERE project.project_id = ?
                """;
        dbAccessor.executeUpdate(sql, created.getId(), projectId);
        return created;
    }

    private ProjectStudioSpecProto updateProjectStudioSpec(long projectId, ProjectStudioSpecProto studioSpec,
            String operator) {
        if (studioSpec.getId() > 0) {
            return updateStudioSpec(studioSpec, operator);
        }
        return createProjectStudioSpec(projectId, studioSpec, operator);
    }

    private ProjectStudioSpecProto createStudioSpec(ProjectStudioSpecProto studioSpec, String operator) {
        Long newId = studioSpecIdGenerator.getNextID();
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                INSERT INTO project_studio_specification (project_studio_spec_id, goals, target_audience, branding_guidelines,
                disliked_design_websites, other_instructions, winning_criteria, submitters_locked_between_rounds, round_one_introduction,
                round_two_introduction, colors, fonts, layout_and_size, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final String goals = Helper.extract(studioSpec::hasGoals, studioSpec::getGoals);
        final String targetAudience = Helper.extract(studioSpec::hasTargetAudience, studioSpec::getTargetAudience);
        final String brandingGuidelines = Helper.extract(studioSpec::hasBrandingGuidelines,
                studioSpec::getBrandingGuidelines);
        final String dislikedDesignWebsites = Helper.extract(studioSpec::hasDislikedDesignWebsites,
                studioSpec::getDislikedDesignWebsites);
        final String otherInstructions = Helper.extract(studioSpec::hasOtherInstructions,
                studioSpec::getOtherInstructions);
        final String winningCriteria = Helper.extract(studioSpec::hasWinningCriteria, studioSpec::getWinningCriteria);
        final Boolean submittersLockedBetweenRounds = Helper.extract(studioSpec::hasSubmittersLockedBetweenRounds,
                studioSpec::getSubmittersLockedBetweenRounds);
        final String roundOneIntroduction = Helper.extract(studioSpec::hasRoundOneIntroduction,
                studioSpec::getRoundOneIntroduction);
        final String roundTwoIntroduction = Helper.extract(studioSpec::hasRoundTwoIntroduction,
                studioSpec::getRoundTwoIntroduction);
        final String colors = Helper.extract(studioSpec::hasColors, studioSpec::getColors);
        final String fonts = Helper.extract(studioSpec::hasFonts, studioSpec::getFonts);
        final String layoutAndSize = Helper.extract(studioSpec::hasLayoutAndSize, studioSpec::getLayoutAndSize);
        dbAccessor.executeUpdate(sql, newId, goals, targetAudience, brandingGuidelines,
                dislikedDesignWebsites, otherInstructions, winningCriteria, submittersLockedBetweenRounds,
                roundOneIntroduction, roundTwoIntroduction, colors, fonts, layoutAndSize, operator, now, operator, now);
        return ProjectStudioSpecProto.newBuilder(studioSpec).setId(newId).setCreateUser(operator).setCreateDate(nowTs)
                .setModifyUser(operator).setModifyDate(nowTs).build();
    }

    private ProjectStudioSpecProto updateStudioSpec(ProjectStudioSpecProto studioSpec, String operator) {
        Date now = new Date();
        Timestamp nowTs = Timestamp.newBuilder().setSeconds(now.toInstant().getEpochSecond()).build();
        String sql = """
                UPDATE project_studio_specification SET goals=?, target_audience=?, branding_guidelines=?, disliked_design_websites=?,
                other_instructions=?, winning_criteria=?, submitters_locked_between_rounds=?, round_one_introduction=?,
                round_two_introduction=?, colors=?, fonts=?, layout_and_size=?, modify_user=?, modify_date=?
                WHERE project_studio_spec_id = ?
                """;
        final String goals = Helper.extract(studioSpec::hasGoals, studioSpec::getGoals);
        final String targetAudience = Helper.extract(studioSpec::hasTargetAudience, studioSpec::getTargetAudience);
        final String brandingGuidelines = Helper.extract(studioSpec::hasBrandingGuidelines,
                studioSpec::getBrandingGuidelines);
        final String dislikedDesignWebsites = Helper.extract(studioSpec::hasDislikedDesignWebsites,
                studioSpec::getDislikedDesignWebsites);
        final String otherInstructions = Helper.extract(studioSpec::hasOtherInstructions,
                studioSpec::getOtherInstructions);
        final String winningCriteria = Helper.extract(studioSpec::hasWinningCriteria, studioSpec::getWinningCriteria);
        final Boolean submittersLockedBetweenRounds = Helper.extract(studioSpec::hasSubmittersLockedBetweenRounds,
                studioSpec::getSubmittersLockedBetweenRounds);
        final String roundOneIntroduction = Helper.extract(studioSpec::hasRoundOneIntroduction,
                studioSpec::getRoundOneIntroduction);
        final String roundTwoIntroduction = Helper.extract(studioSpec::hasRoundTwoIntroduction,
                studioSpec::getRoundTwoIntroduction);
        final String colors = Helper.extract(studioSpec::hasColors, studioSpec::getColors);
        final String fonts = Helper.extract(studioSpec::hasFonts, studioSpec::getFonts);
        final String layoutAndSize = Helper.extract(studioSpec::hasLayoutAndSize, studioSpec::getLayoutAndSize);
        dbAccessor.executeUpdate(sql, goals, targetAudience, brandingGuidelines, dislikedDesignWebsites,
                otherInstructions, winningCriteria, submittersLockedBetweenRounds, roundOneIntroduction,
                roundTwoIntroduction, colors, fonts, layoutAndSize, operator, now, studioSpec.getId());
        return ProjectStudioSpecProto.newBuilder(studioSpec).setModifyUser(operator).setModifyDate(nowTs).build();
    }

    public int removeStudioSpecFromProjects(long specId) {
        String sql = """
                UPDATE project SET project_studio_spec_id = NULL WHERE project_studio_spec_id = ?
                """;
        return dbAccessor.executeUpdate(sql, specId);
    }

    private List<ProjectPropertyTypeProto> getAllProjectPropertyTypes() {
        String sql = """
                SELECT project_info_type_id, name, description FROM project_info_type_lu
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            ProjectPropertyTypeProto.Builder builder = ProjectPropertyTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setDescription);
            return builder.build();
        });
    }

    private Map<String, Long> makePropertyNamePropertyIdMap(List<ProjectPropertyTypeProto> propertyTypes) {
        return propertyTypes.stream()
                .collect(Collectors.toMap(ProjectPropertyTypeProto::getName, ProjectPropertyTypeProto::getId));
    }

    private Map<Long, String> makePropertyIdPropertyValueMap(List<ProjectPropertyProto> properties) {
        return properties.stream()
                .collect(Collectors.toMap(ProjectPropertyProto::getId, ProjectPropertyProto::getValue));
    }

    private List<Long> getProjectIdsByFileType(long fileTypeId) {
        String sql = """
                SELECT DISTINCT project_id FROM project_file_type_xref WHERE file_type_id = ?
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, fileTypeId);
    }

    public List<Long> getProjectIdsByPrizeId(long prizeId) {
        String sql = """
                SELECT project_id FROM prize WHERE prize_id = ?
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, prizeId);
    }

    public List<Long> getProjectIdsByStudioSpecId(long specId) {
        String sql = """
                SELECT DISTINCT project_id FROM project WHERE project_studio_spec_id = ?
                """;
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        }, specId);
    }

    private void auditProjects(List<Long> projectIds, String reason, String operator) {
        for (long id : projectIds) {
            auditProject(id, reason, operator);
        }
    }

    private int auditProject(long projectId, String reason, String operator) {
        Long newId = projectAuditIdGenerator.getNextID();
        String sql = """
                INSERT INTO project_audit (project_audit_id, project_id, update_reason, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, CURRENT, ?, CURRENT)
                    """;
        return dbAccessor.executeUpdate(sql, newId, projectId, reason, operator, operator);
    }

    private int auditProjectInfo(long projectId, int auditType, long typeId, String value, String operator) {
        String sql = """
                INSERT INTO project_info_audit (project_id, project_info_type_id, value, audit_action_type_id, action_date, action_user_id)
                VALUES (?, ?, ?, ?, CURRENT, ?)
                """;
        return dbAccessor.executeUpdate(sql, projectId, typeId, value, auditType, operator);
    }

    /* #region */
    private boolean isProjectExists(long id) {
        return checkEntityExists("project", "project_id", id);
    }

    private boolean isFileTypeExists(long id) {
        return checkEntityExists("file_type_lu", "file_type_id", id);
    }

    private boolean isPrizeExists(long id) {
        return checkEntityExists("prize", "prize_id", id);
    }

    private boolean isStudioSpecExists(long id) {
        return checkEntityExists("project_studio_specification", "project_studio_spec_id", id);
    }

    private boolean checkEntityExists(String tableName, String columnName, long id) {
        String sql = """
                SELECT CASE WHEN EXISTS (SELECT 1 from %s where %s = ?) THEN 1 ELSE 0 END FROM DUAL
                """.formatted(tableName, columnName);
        return dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getObject(1).equals(1);
        }, id).get(0);
    }
    /* #endregion */

    private void validateCreateProjectRequest(CreateProjectRequest request, Map<String, Long> propertyTypeNameIdMap) {
        Helper.assertObjectNotNull(request::hasProject, "project");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        ProjectProto project = request.getProject();
        Helper.assertObjectNotNull(project::hasProjectStatus, "project_status");
        Helper.assertObjectNotNull(project.getProjectStatus()::hasId, "project_status_id");
        Helper.assertObjectNotNull(project::hasProjectCategory, "project_category");
        Helper.assertObjectNotNull(project.getProjectCategory()::hasId, "project_category_id");
        for (ProjectPropertyProto property : project.getPropertiesList()) {
            if (!propertyTypeNameIdMap.containsKey(property.getName())) {
                throw new IllegalArgumentException(
                        "Project property type with name '%s' cannot be found".formatted(property.getName()));
            }
        }
        validateProjectFileTypes(project.getFileTypesList());
        for (PrizeProto prize : project.getPrizesList()) {
            validateProjectPrize(prize);
        }
    }

    private void validateUpdateProjectRequest(UpdateProjectRequest request, Map<String, Long> propertyTypeNameIdMap) {
        Helper.assertObjectNotNull(request::hasProject, "project");
        Helper.assertObjectNotNull(request::hasReason, "reason");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        ProjectProto project = request.getProject();
        Helper.assertObjectNotNull(project::hasId, "id");
        Helper.assertObjectNotNull(project::hasProjectStatus, "project_status");
        Helper.assertObjectNotNull(project.getProjectStatus()::hasId, "project_status_id");
        Helper.assertObjectNotNull(project::hasProjectCategory, "project_category");
        Helper.assertObjectNotNull(project.getProjectCategory()::hasId, "project_category_id");
        if (!isProjectExists(project.getId())) {
            throw new IllegalArgumentException(
                    "Project with id '%s' cannot be found".formatted(project.getId()));
        }
        for (ProjectPropertyProto property : project.getPropertiesList()) {
            if (!propertyTypeNameIdMap.containsKey(property.getName())) {
                throw new IllegalArgumentException(
                        "Project property type with name '%s' cannot be found".formatted(property.getName()));
            }
        }
        validateProjectFileTypes(project.getFileTypesList());
        for (PrizeProto prize : project.getPrizesList()) {
            if (prize.getId() > 0) {
                if (!isPrizeExists(prize.getId())) {
                    throw new IllegalArgumentException(
                            "Prize with id '%s' cannot be found".formatted(prize.getId()));
                }
            }
            validateProjectPrize(prize);
        }
        if (project.getProjectStudioSpec().getId() > 0 && !isStudioSpecExists(project.getProjectStudioSpec().getId())) {
            throw new IllegalArgumentException(
                    "Project Studio Spec with id '%s' cannot be found"
                            .formatted(project.getProjectStudioSpec().getId()));
        }
    }

    private void validateIdProto(IdProto request) {
        Helper.assertObjectNotNull(request::hasId, "id");
    }

    private void validateProjectIdProto(ProjectIdProto request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
    }

    private void validateUpdateProjectFileTypesRequest(UpdateProjectFileTypesRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        if (!isProjectExists(request.getProjectId())) {
            throw new IllegalArgumentException(
                    "Project with id '%s' cannot be found".formatted(request.getProjectId()));
        }
        validateProjectFileTypes(request.getFileTypesList());
    }

    private void validateProjectFileTypes(List<FileTypeProto> fileTypes) {
        for (FileTypeProto fileType : fileTypes) {
            if (fileType.getId() > 0) {
                if (!isFileTypeExists(fileType.getId())) {
                    throw new IllegalArgumentException(
                            "File type with id '%s' cannot be found".formatted(fileType.getId()));
                }
            } else {
                validateFileType(fileType);
            }
        }
    }

    private void validateUpdateProjectPrizesRequest(UpdateProjectPrizesRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        if (!isProjectExists(request.getProjectId())) {
            throw new IllegalArgumentException(
                    "Project with id '%s' cannot be found".formatted(request.getProjectId()));
        }
        for (PrizeProto prize : request.getPrizesList()) {
            if (prize.getId() > 0) {
                if (!isPrizeExists(prize.getId())) {
                    throw new IllegalArgumentException(
                            "Prize with id '%s' cannot be found".formatted(prize.getId()));
                }
            }
            validateProjectPrize(prize);

        }
    }

    private void validateFileType(FileTypeProto fileType) {
        Helper.assertObjectNotNull(fileType::hasDescription, "description");
        Helper.assertObjectNotNull(fileType::hasSort, "sort");
        Helper.assertObjectNotNull(fileType::hasImageFile, "image_file");
        Helper.assertObjectNotNull(fileType::hasExtension, "extension");
        Helper.assertObjectNotNull(fileType::hasBundledFile, "bundled_file");
    }

    private void validateProjectPrize(PrizeProto prize) {
        Helper.assertObjectNotNull(prize::hasPlace, "place");
        Helper.assertObjectNotNull(prize::hasPrizeAmount, "prize_amount");
        Helper.assertObjectNotNull(prize.getPrizeType()::hasId, "prize_type_id");
        Helper.assertObjectNotNull(prize::hasNumberOfSubmissions, "number_of_submissions");
    }

    private void validateCreateFileTypeRequest(CreateFileTypeRequest request) {
        FileTypeProto fileType = request.getFileType();
        validateFileType(fileType);
    }

    private void validateUpdateFileTypeRequest(UpdateFileTypeRequest request) {
        FileTypeProto fileType = request.getFileType();
        Helper.assertObjectNotNullAndPositive(fileType::hasId, fileType::getId, "id");
        if (!isFileTypeExists(fileType.getId())) {
            throw new IllegalArgumentException(
                    "File type with id '%s' cannot be found".formatted(fileType.getId()));
        }
        validateFileType(fileType);
    }

    private void validateDeleteFileTypeRequest(DeleteFileTypeRequest request) {
        Helper.assertObjectNotNull(request::hasFileTypeId, "file_type");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        if (!isFileTypeExists(request.getFileTypeId())) {
            throw new IllegalArgumentException(
                    "File type with id '%s' cannot be found".formatted(request.getFileTypeId()));
        }
    }

    private void validateCreatePrizeRequest(CreatePrizeRequest request) {
        PrizeProto prize = request.getPrize();
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        Helper.assertObjectNotNull(prize::hasProjectId, "project_id");
        validateProjectPrize(prize);
    }

    private void validateUpdatePrizeRequest(UpdatePrizeRequest request) {
        PrizeProto prize = request.getPrize();
        Helper.assertObjectNotNull(prize::hasId, "id");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        Helper.assertObjectNotNull(prize::hasProjectId, "project_id");
        validateProjectPrize(prize);
        if (!isPrizeExists(prize.getId())) {
            throw new IllegalArgumentException(
                    "Prize with id '%s' cannot be found".formatted(prize.getId()));
        }
    }

    private void validateDeletePrizeRequest(DeletePrizeRequest request) {
        Helper.assertObjectNotNull(request::hasPrizeId, "prize_id");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        if (!isPrizeExists(request.getPrizeId())) {
            throw new IllegalArgumentException(
                    "Prize with id '%s' cannot be found".formatted(request.getPrizeId()));
        }
    }

    private void validateCreateStudioSpecRequest(CreateStudioSpecRequest request) {
        Helper.assertObjectNotNull(request::hasOperator, "operator");
    }

    private void validateUpdateStudioSpecRequest(UpdateStudioSpecRequest request) {
        Helper.assertObjectNotNull(request.getProjectStudioSpec()::hasId, "id");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        if (!isStudioSpecExists(request.getProjectStudioSpec().getId())) {
            throw new IllegalArgumentException(
                    "Project Studio Spec with id '%s' cannot be found"
                            .formatted(request.getProjectStudioSpec().getId()));
        }
    }

    private void validateDeleteStudioSpecRequest(DeleteStudioSpecRequest request) {
        Helper.assertObjectNotNull(request::hasProjectStudioSpecId, "project_studio_spec_id");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        if (!isStudioSpecExists(request.getProjectStudioSpecId())) {
            throw new IllegalArgumentException(
                    "Project Studio Spec with id '%s' cannot be found"
                            .formatted(request.getProjectStudioSpecId()));
        }
    }

    private void validateUpdateProjectStudioSpecRequest(UpdateProjectStudioSpecRequest request) {
        Helper.assertObjectNotNull(request::hasProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
        if (request.getProjectStudioSpec().getId() > 0 && !isStudioSpecExists(request.getProjectStudioSpec().getId())) {
            throw new IllegalArgumentException(
                    "Project Studio Spec with id '%s' cannot be found"
                            .formatted(request.getProjectStudioSpec().getId()));
        }
    }

    private void validateGetProjectsRequest(GetProjectsRequest request) {
        Helper.assertObjectNotEmpty(request::getProjectIdsCount, "project_ids");
    }
}