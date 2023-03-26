package com.topcoder.or.repository;

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
import com.topcoder.onlinereview.grpc.scorecard.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;
import com.topcoder.or.util.SearchBundleHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ScorecardService extends ScorecardServiceGrpc.ScorecardServiceImplBase {
    private final DBAccessor dbAccessor;
    private final DBHelper dbHelper;
    private final SearchBundleManager searchBundleManager;

    private static final String SCORECARD_SEARCH_BUNDLE_NAME = "ScorecardSearchBundle";

    private SearchBundle searchBundle;
    private IDGenerator groupIdGenerator;
    private IDGenerator questionIdGenerator;
    private IDGenerator scorecardIdGenerator;
    private IDGenerator sectionIdGenerator;

    private static final String SCORECARD_GROUP_ID_SEQUENCE = "scorecard_group_id_seq";
    private static final String SCORECARD_QUESTION_ID_SEQUENCE = "scorecard_question_id_seq";
    private static final String SCORECARD_ID_SEQUENCE = "scorecard_id_seq";
    private static final String SCORECARD_SECTION_ID_SEQUENCE = "scorecard_section_id_seq";

    public ScorecardService(DBAccessor dbAccessor, DBHelper dbHelper, SearchBundleManager searchBundleManager) {
        this.dbAccessor = dbAccessor;
        this.dbHelper = dbHelper;
        this.searchBundleManager = searchBundleManager;
    }

    @PostConstruct
    public void postRun() {
        searchBundle = searchBundleManager.getSearchBundle(SCORECARD_SEARCH_BUNDLE_NAME);
        groupIdGenerator = new IDGenerator(SCORECARD_GROUP_ID_SEQUENCE, dbHelper);
        questionIdGenerator = new IDGenerator(SCORECARD_QUESTION_ID_SEQUENCE, dbHelper);
        scorecardIdGenerator = new IDGenerator(SCORECARD_ID_SEQUENCE, dbHelper);
        sectionIdGenerator = new IDGenerator(SCORECARD_SECTION_ID_SEQUENCE, dbHelper);
        SearchBundleHelper.setSearchableFields(searchBundle, SearchBundleHelper.SCORECARD_SEARCH_BUNDLE);
    }

    @Override
    public void createGroup(CreateGroupRequest request, StreamObserver<GroupIdProto> responseObserver) {
        validateCreateGroupRequest(request);
        long newId = groupIdGenerator.getNextID();
        createGroup(request, newId);
        responseObserver.onNext(GroupIdProto.newBuilder().setScorecardGroupId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createGroups(CreateGroupsRequest request, StreamObserver<GroupIdsProto> responseObserver) {
        validateCreateGroupsRequest(request);
        List<Long> groupIds = generateIds(request.getGroupsCount(), groupIdGenerator);
        int i = 0;
        for (CreateGroupRequest group : request.getGroupsList()) {
            createGroup(group, groupIds.get(i));
            i++;
        }
        responseObserver.onNext(GroupIdsProto.newBuilder().addAllScorecardGroupIds(groupIds).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateGroup(UpdateGroupRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateGroupRequest(request);
        String sql = """
                UPDATE scorecard_group
                SET scorecard_id = ?, name = ?, weight = ?, sort = ?, modify_user = ?, modify_date = CURRENT
                WHERE scorecard_group_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getScorecardId(), request.getName(), request.getWeight(),
                request.getSort(), request.getOperator(), request.getScorecardGroupId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getScorecardSectionIds(GroupIdsProto request, StreamObserver<SectionIdsProto> responseObserver) {
        validateGroupIdsProto(request);
        String sql = """
                SELECT scorecard_section_id
                FROM scorecard_section
                WHERE scorecard_group_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getScorecardGroupIdsCount());
        List<Long> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return rs.getLong(1);
        }, request.getScorecardGroupIdsList().toArray());
        responseObserver.onNext(SectionIdsProto.newBuilder().addAllScorecardSectionIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteGroups(GroupIdsProto request, StreamObserver<CountProto> responseObserver) {
        validateGroupIdsProto(request);
        String sql = """
                DELETE FROM scorecard_group
                WHERE scorecard_group_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getScorecardGroupIdsCount());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), request.getScorecardGroupIdsList().toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getGroup(GroupIdProto request, StreamObserver<GetGroupResponse> responseObserver) {
        validateGroupIdProto(request);
        String sql = """
                SELECT scorecard_group_id, name, weight
                FROM scorecard_group
                WHERE scorecard_group_id = ?
                """;
        List<GetGroupResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetGroupResponse.Builder builder = GetGroupResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardGroupId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetFloat(rs, 3, builder::setWeight);
            return builder.build();
        }, request.getScorecardGroupId());
        responseObserver.onNext(result.isEmpty() ? GetGroupResponse.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getGroups(ScorecardIdProto request, StreamObserver<GetGroupsResponse> responseObserver) {
        validateScorecardIdProto(request);
        String sql = """
                SELECT scorecard_group_id, name, weight
                FROM scorecard_group
                WHERE scorecard_id = ? ORDER BY sort
                """;
        List<GetGroupResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetGroupResponse.Builder builder = GetGroupResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardGroupId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetFloat(rs, 3, builder::setWeight);
            return builder.build();
        }, request.getScorecardId());
        responseObserver.onNext(GetGroupsResponse.newBuilder().addAllGroups(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createQuestion(CreateQuestionRequest request, StreamObserver<QuestionIdProto> responseObserver) {
        validateCreateQuestionRequest(request);
        long newId = questionIdGenerator.getNextID();
        createQuestion(request, newId);
        responseObserver.onNext(QuestionIdProto.newBuilder().setScorecardQuestionId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createQuestions(CreateQuestionsRequest request, StreamObserver<QuestionIdsProto> responseObserver) {
        validateCreateQuestionsRequest(request);
        List<Long> questionIds = generateIds(request.getQuestionsCount(), questionIdGenerator);
        int i = 0;
        for (CreateQuestionRequest question : request.getQuestionsList()) {
            createQuestion(question, questionIds.get(i));
            i++;
        }
        responseObserver.onNext(QuestionIdsProto.newBuilder().addAllScorecardQuestionIds(questionIds).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateQuestion(UpdateQuestionRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateQuestionRequest(request);
        String sql = """
                UPDATE scorecard_question SET scorecard_question_type_id = ?, scorecard_section_id = ?, description = ?, guideline = ?,
                weight = ?, sort = ?, upload_document = ?, upload_document_required = ?, modify_user = ?, modify_date = CURRENT
                WHERE scorecard_question_id = ?
                """;
        String guideline = Helper.extract(request::hasGuideline, request::getGuideline);
        int affected = dbAccessor.executeUpdate(sql, request.getScorecardQuestionTypeId(),
                request.getScorecardSectionId(), request.getDescription(), guideline, request.getWeight(),
                request.getSort(), request.getUploadDocument(), request.getUploadDocumentRequired(),
                request.getOperator());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteQuestions(QuestionIdsProto request, StreamObserver<CountProto> responseObserver) {
        validateQuestionIdsProto(request);
        String sql = """
                DELETE FROM scorecard_question
                WHERE scorecard_question_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getScorecardQuestionIdsCount());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), request.getScorecardQuestionIdsList().toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getQuestion(QuestionIdProto request, StreamObserver<GetQuestionResponse> responseObserver) {
        validateQuestionIdProto(request);
        String sql = """
                SELECT sq.scorecard_question_id, sq.description, sq.guideline, sq.weight, sq.upload_document, sq.upload_document_required,
                type.scorecard_question_type_id, type.name AS TypeName
                FROM scorecard_question AS sq JOIN scorecard_question_type_lu AS type ON sq.scorecard_question_type_id = type.scorecard_question_type_id
                WHERE sq.scorecard_question_id = ?
                """;
        List<GetQuestionResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetQuestionResponse.Builder builder = GetQuestionResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardQuestionId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDescription);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setGuideline);
            ResultSetHelper.applyResultSetFloat(rs, 4, builder::setWeight);
            ResultSetHelper.applyResultSetBool(rs, 5, builder::setUploadDocument);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setUploadDocumentRequired);
            ResultSetHelper.applyResultSetLong(rs, 7, builder::setScorecardQuestionTypeId);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setScorecardQuestionTypeName);
            return builder.build();
        }, request.getScorecardQuestionId());
        responseObserver.onNext(result.isEmpty() ? GetQuestionResponse.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getQuestions(SectionIdProto request, StreamObserver<GetQuestionsResponse> responseObserver) {
        validateSectionIdProto(request);
        String sql = """
                SELECT sq.scorecard_question_id, sq.description, sq.guideline, sq.weight, sq.upload_document, sq.upload_document_required,
                type.scorecard_question_type_id, type.name AS TypeName
                FROM scorecard_question AS sq JOIN scorecard_question_type_lu AS type ON sq.scorecard_question_type_id = type.scorecard_question_type_id
                WHERE sq.scorecard_section_id = ?
                ORDER BY sort
                """;
        List<GetQuestionResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetQuestionResponse.Builder builder = GetQuestionResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardQuestionId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDescription);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setGuideline);
            ResultSetHelper.applyResultSetFloat(rs, 4, builder::setWeight);
            ResultSetHelper.applyResultSetBool(rs, 5, builder::setUploadDocument);
            ResultSetHelper.applyResultSetBool(rs, 6, builder::setUploadDocumentRequired);
            ResultSetHelper.applyResultSetLong(rs, 7, builder::setScorecardQuestionTypeId);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setScorecardQuestionTypeName);
            return builder.build();
        }, request.getScorecardSectionId());
        responseObserver.onNext(GetQuestionsResponse.newBuilder().addAllQuestions(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createScorecard(CreateScorecardRequest request, StreamObserver<ScorecardIdProto> responseObserver) {
        validateCreateScorecardRequest(request);
        long newId = scorecardIdGenerator.getNextID();
        createScorecard(request, newId);
        responseObserver.onNext(ScorecardIdProto.newBuilder().setScorecardId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateScorecard(UpdateScorecardRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateScorecardRequest(request);
        String sql = """
                UPDATE scorecard
                SET scorecard_status_id = ?, scorecard_type_id = ?, project_category_id = ?, name = ?, version = ?,
                min_score = ?, max_score = ?, modify_user = ?, modify_date = ?
                WHERE scorecard_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getScorecardStatusId(), request.getScorecardTypeId(),
                request.getProjectCategoryId(), request.getName(), request.getVersion(), request.getMinScore(),
                request.getMaxScore(), request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getScorecardId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllScorecardTypes(Empty request, StreamObserver<GetAllScorecardTypesResponse> responseObserver) {
        String sql = """
                SELECT scorecard_type_id, name
                FROM scorecard_type_lu
                """;
        List<ScorecardTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ScorecardTypeProto.Builder builder = ScorecardTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            return builder.build();
        });
        responseObserver.onNext(GetAllScorecardTypesResponse.newBuilder().addAllScorecardTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllQuestionTypes(Empty request, StreamObserver<GetAllQuestionTypesResponse> responseObserver) {
        String sql = """
                SELECT scorecard_question_type_id, name
                FROM scorecard_question_type_lu
                """;
        List<QuestionTypeProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            QuestionTypeProto.Builder builder = QuestionTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardQuestionTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            return builder.build();
        });
        responseObserver.onNext(GetAllQuestionTypesResponse.newBuilder().addAllQuestionTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllScorecardStatuses(Empty request,
            StreamObserver<GetAllScorecardStatusesResponse> responseObserver) {
        String sql = """
                SELECT scorecard_status_id, name
                FROM scorecard_status_lu
                """;
        List<ScorecardStatusProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ScorecardStatusProto.Builder builder = ScorecardStatusProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardStatusId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            return builder.build();
        });
        responseObserver.onNext(GetAllScorecardStatusesResponse.newBuilder().addAllScorecardStatuses(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDefaultScorecardsIDInfo(ProjectCategoryIdProto request,
            StreamObserver<GetDefaultScorecardsIDInfoResponse> responseObserver) {
        validateProjectCategoryIdProto(request);
        String sql = """
                SELECT scorecard_type_id, scorecard_id
                FROM default_scorecard
                WHERE project_category_id = ?
                """;
        List<ScorecardsIDInfoProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            ScorecardsIDInfoProto.Builder builder = ScorecardsIDInfoProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardTypeId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setScorecardId);
            return builder.build();
        });
        responseObserver.onNext(GetDefaultScorecardsIDInfoResponse.newBuilder().addAllScorecardIdInfos(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getScorecards(ScorecardIdsProto request, StreamObserver<GetScorecardsResponse> responseObserver) {
        validateScorecardIdsProto(request);
        String sql = """
                SELECT sc.scorecard_id, status.scorecard_status_id as status_id, type.scorecard_type_id as type_id, sc.project_category_id,
                sc.name as scorecard_name, sc.version, sc.min_score, sc.max_score, sc.create_user, sc.create_date,
                sc.modify_user, sc.modify_date, status.name AS status_name, type.name AS type_name
                FROM scorecard AS sc JOIN scorecard_type_lu AS type ON sc.scorecard_type_id=type.scorecard_type_id
                JOIN scorecard_status_lu AS status ON sc.scorecard_status_id=status.scorecard_status_id
                WHERE sc.scorecard_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getScorecardIdsCount());
        List<ScorecardProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            ScorecardProto.Builder builder = ScorecardProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setScorecardStatusId);
            ResultSetHelper.applyResultSetLong(rs, 3, builder::setScorecardTypeId);
            ResultSetHelper.applyResultSetLong(rs, 4, builder::setProjectCategoryId);
            ResultSetHelper.applyResultSetString(rs, 5, builder::setName);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setVersion);
            ResultSetHelper.applyResultSetFloat(rs, 7, builder::setMinScore);
            ResultSetHelper.applyResultSetFloat(rs, 8, builder::setMaxScore);
            ResultSetHelper.applyResultSetString(rs, 9, builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 10, builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, 11, builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, 12, builder::setModifyDate);
            ResultSetHelper.applyResultSetString(rs, 13, builder::setScorecardStatusName);
            ResultSetHelper.applyResultSetString(rs, 14, builder::setScorecardTypeName);
            return builder.build();
        }, request.getScorecardIdsList().toArray());
        responseObserver.onNext(GetScorecardsResponse.newBuilder().addAllScorecards(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getScorecardsInUse(ScorecardIdsProto request, StreamObserver<ScorecardIdsProto> responseObserver) {
        validateScorecardIdsProto(request);
        String sql = """
                SELECT pc.parameter
                FROM phase_criteria pc
                JOIN phase_criteria_type_lu pct ON pc.phase_criteria_type_id = pct.phase_criteria_type_id
                WHERE pct.name='Scorecard ID' AND pc.parameter IN (%s)
                """;
        String inSql = Helper.getInClause(request.getScorecardIdsCount());
        List<Long> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return rs.getLong(1);
        }, request.getScorecardIdsList().toArray());
        responseObserver.onNext(ScorecardIdsProto.newBuilder().addAllScorecardIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createSection(CreateSectionRequest request, StreamObserver<SectionIdProto> responseObserver) {
        validateCreateSectionRequest(request);
        long newId = sectionIdGenerator.getNextID();
        createSection(request, newId);
        responseObserver.onNext(SectionIdProto.newBuilder().setScorecardSectionId(newId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void createSections(CreateSectionsRequest request, StreamObserver<SectionIdsProto> responseObserver) {
        validateCreateSectionsRequest(request);
        List<Long> sectionIds = generateIds(request.getSectionsCount(), sectionIdGenerator);
        int i = 0;
        for (CreateSectionRequest section : request.getSectionsList()) {
            createSection(section, sectionIds.get(i));
            i++;
        }
        responseObserver.onNext(SectionIdsProto.newBuilder().addAllScorecardSectionIds(sectionIds).build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateSection(UpdateSectionRequest request, StreamObserver<CountProto> responseObserver) {
        validateUpdateSectionRequest(request);
        String sql = """
                UPDATE scorecard_section
                SET scorecard_group_id = ?, name = ?, weight = ?, sort = ?, modify_user = ?, modify_date = CURRENT
                WHERE scorecard_section_id = ?
                """;
        int affected = dbAccessor.executeUpdate(sql, request.getScorecardGroupId(), request.getName(),
                request.getWeight(), request.getSort(), request.getOperator(), request.getScorecardSectionId());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getQuestionIds(SectionIdsProto request, StreamObserver<QuestionIdsProto> responseObserver) {
        validateSectionIdsProto(request);
        String sql = """
                SELECT scorecard_question_id
                FROM scorecard_question
                WHERE scorecard_section_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getScorecardSectionIdsCount());
        List<Long> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return rs.getLong(1);
        }, request.getScorecardSectionIdsList().toArray());
        responseObserver.onNext(QuestionIdsProto.newBuilder().addAllScorecardQuestionIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSections(SectionIdsProto request, StreamObserver<CountProto> responseObserver) {
        validateSectionIdsProto(request);
        String sql = """
                DELETE FROM scorecard_section
                WHERE scorecard_section_id IN  (%s)
                """;
        String inSql = Helper.getInClause(request.getScorecardSectionIdsCount());
        int affected = dbAccessor.executeUpdate(sql.formatted(inSql), request.getScorecardSectionIdsList().toArray());
        responseObserver.onNext(CountProto.newBuilder().setCount(affected).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getSection(SectionIdProto request, StreamObserver<GetSectionResponse> responseObserver) {
        validateSectionIdProto(request);
        String sql = """
                SELECT scorecard_section_id, name, weight
                FROM scorecard_section
                WHERE scorecard_section_id = ?
                """;
        List<GetSectionResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetSectionResponse.Builder builder = GetSectionResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardSectionId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetFloat(rs, 3, builder::setWeight);
            return builder.build();
        }, request.getScorecardSectionId());
        responseObserver.onNext(result.isEmpty() ? GetSectionResponse.getDefaultInstance() : result.get(0));
        responseObserver.onCompleted();
    }

    @Override
    public void getSections(GroupIdProto request, StreamObserver<GetSectionsResponse> responseObserver) {
        validateGroupIdProto(request);
        String sql = """
                SELECT scorecard_section_id, name, weight
                FROM scorecard_section
                WHERE scorecard_group_id = ?
                ORDER BY sort
                """;
        List<GetSectionResponse> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            GetSectionResponse.Builder builder = GetSectionResponse.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setScorecardSectionId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setName);
            ResultSetHelper.applyResultSetFloat(rs, 3, builder::setWeight);
            return builder.build();
        }, request.getScorecardGroupId());
        responseObserver.onNext(GetSectionsResponse.newBuilder().addAllSections(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchScorecards(FilterProto request, StreamObserver<GetScorecardsResponse> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<ScorecardProto> result = searchBundle.search(filter, (rs, _i) -> {
            ScorecardProto.Builder builder = ScorecardProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, "scorecard_id", builder::setScorecardId);
            ResultSetHelper.applyResultSetLong(rs, "status_id", builder::setScorecardStatusId);
            ResultSetHelper.applyResultSetLong(rs, "type_id", builder::setScorecardTypeId);
            ResultSetHelper.applyResultSetLong(rs, "project_category_id", builder::setProjectCategoryId);
            ResultSetHelper.applyResultSetString(rs, "status_name", builder::setName);
            ResultSetHelper.applyResultSetString(rs, "version", builder::setVersion);
            ResultSetHelper.applyResultSetFloat(rs, "min_score", builder::setMinScore);
            ResultSetHelper.applyResultSetFloat(rs, "max_score", builder::setMaxScore);
            ResultSetHelper.applyResultSetString(rs, "create_user", builder::setCreateUser);
            ResultSetHelper.applyResultSetTimestamp(rs, "create_date", builder::setCreateDate);
            ResultSetHelper.applyResultSetString(rs, "modify_user", builder::setModifyUser);
            ResultSetHelper.applyResultSetTimestamp(rs, "modify_date", builder::setModifyDate);
            ResultSetHelper.applyResultSetString(rs, "status_name", builder::setScorecardStatusName);
            ResultSetHelper.applyResultSetString(rs, "type_name", builder::setScorecardTypeName);
            return builder.build();
        });
        responseObserver.onNext(GetScorecardsResponse.newBuilder().addAllScorecards(result).build());
        responseObserver.onCompleted();
    }

    private List<Long> generateIds(int length, IDGenerator idGenerator) {
        List<Long> ids = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            ids.add(idGenerator.getNextID());
        }
        return ids;
    }

    private int createGroup(CreateGroupRequest request, long scorecardGroupId) {
        String sql = """
                INSERT INTO scorecard_group (scorecard_group_id, scorecard_id, name, weight, sort, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT, ?, CURRENT)
                """;
        return dbAccessor.executeUpdate(sql, scorecardGroupId, request.getScorecardId(), request.getName(),
                request.getWeight(), request.getSort(), request.getOperator(), request.getOperator());
    }

    private int createQuestion(CreateQuestionRequest request, long scorecardQuestionId) {
        String sql = """
                INSERT INTO scorecard_question (scorecard_question_id, scorecard_question_type_id, scorecard_section_id, description, guideline,
                weight, sort, upload_document, upload_document_required, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT, ?, CURRENT)
                """;
        String guideline = Helper.extract(request::hasGuideline, request::getGuideline);
        return dbAccessor.executeUpdate(sql, scorecardQuestionId, request.getScorecardQuestionTypeId(),
                request.getScorecardSectionId(), request.getDescription(), guideline, request.getWeight(),
                request.getSort(), request.getUploadDocument(), request.getUploadDocumentRequired(),
                request.getOperator(), request.getOperator());
    }

    private int createScorecard(CreateScorecardRequest request, long scorecardId) {
        String sql = """
                INSERT INTO scorecard(scorecard_id, scorecard_status_id, scorecard_type_id, project_category_id, name, version,
                min_score, max_score, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        return dbAccessor.executeUpdate(sql, scorecardId, request.getScorecardStatusId(), request.getScorecardTypeId(),
                request.getProjectCategoryId(), request.getName(), request.getVersion(), request.getMinScore(),
                request.getMaxScore(), request.getCreateUser(), Helper.convertDate(request.getCreateDate()),
                request.getModifyUser(), Helper.convertDate(request.getModifyDate()));
    }

    private int createSection(CreateSectionRequest request, long scorecardSectionId) {
        String sql = """
                INSERT INTO scorecard_section (scorecard_section_id, scorecard_group_id, name, weight, sort, create_user, create_date, modify_user, modify_date)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT, ?, CURRENT)
                """;
        return dbAccessor.executeUpdate(sql, scorecardSectionId, request.getScorecardGroupId(), request.getName(),
                request.getWeight(), request.getSort(), request.getOperator(), request.getOperator());
    }

    private void validateCreateGroupRequest(CreateGroupRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardId, "scorecard_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasWeight, "weight");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
    }

    private void validateCreateGroupsRequest(CreateGroupsRequest request) {
        Helper.assertObjectNotEmpty(request::getGroupsCount, "groups");
        for (CreateGroupRequest createGroupRequest : request.getGroupsList()) {
            validateCreateGroupRequest(createGroupRequest);
        }
    }

    private void validateUpdateGroupRequest(UpdateGroupRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardGroupId, "scorecard_group_id");
        Helper.assertObjectNotNull(request::hasScorecardId, "scorecard_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasWeight, "weight");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
    }

    private void validateGroupIdsProto(GroupIdsProto request) {
        Helper.assertObjectNotEmpty(request::getScorecardGroupIdsCount, "scorecard_group_ids");
    }

    private void validateGroupIdProto(GroupIdProto request) {
        Helper.assertObjectNotNull(request::hasScorecardGroupId, "scorecard_group_id");
    }

    private void validateScorecardIdProto(ScorecardIdProto request) {
        Helper.assertObjectNotNull(request::hasScorecardId, "scorecard_id");
    }

    private void validateCreateQuestionRequest(CreateQuestionRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardQuestionTypeId, "scorecard_question_type_id");
        Helper.assertObjectNotNull(request::hasScorecardSectionId, "scorecard_section_id");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasWeight, "weight");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasUploadDocument, "upload_document");
        Helper.assertObjectNotNull(request::hasUploadDocumentRequired, "upload_document_required");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
    }

    private void validateCreateQuestionsRequest(CreateQuestionsRequest request) {
        Helper.assertObjectNotEmpty(request::getQuestionsCount, "questions");
        for (CreateQuestionRequest createQuestionRequest : request.getQuestionsList()) {
            validateCreateQuestionRequest(createQuestionRequest);
        }
    }

    private void validateUpdateQuestionRequest(UpdateQuestionRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardQuestionId, "scorecard_question_id");
        Helper.assertObjectNotNull(request::hasScorecardQuestionTypeId, "scorecard_question_type_id");
        Helper.assertObjectNotNull(request::hasScorecardSectionId, "scorecard_section_id");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasWeight, "weight");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasUploadDocument, "upload_document");
        Helper.assertObjectNotNull(request::hasUploadDocumentRequired, "upload_document_required");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
    }

    private void validateQuestionIdsProto(QuestionIdsProto request) {
        Helper.assertObjectNotEmpty(request::getScorecardQuestionIdsCount, "scorecard_question_ids");
    }

    private void validateQuestionIdProto(QuestionIdProto request) {
        Helper.assertObjectNotNull(request::hasScorecardQuestionId, "scorecard_question_id");
    }

    private void validateSectionIdProto(SectionIdProto request) {
        Helper.assertObjectNotNull(request::hasScorecardSectionId, "scorecard_section_id");
    }

    private void validateCreateScorecardRequest(CreateScorecardRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardStatusId, "scorecard_status_id");
        Helper.assertObjectNotNull(request::hasScorecardTypeId, "scorecard_type_id");
        Helper.assertObjectNotNull(request::hasProjectCategoryId, "project_category_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasVersion, "version");
        Helper.assertObjectNotNull(request::hasMinScore, "min_score");
        Helper.assertObjectNotNull(request::hasMaxScore, "max_score");
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUpdateScorecardRequest(UpdateScorecardRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardId, "scorecard_id");
        Helper.assertObjectNotNull(request::hasScorecardStatusId, "scorecard_status_id");
        Helper.assertObjectNotNull(request::hasScorecardTypeId, "scorecard_type_id");
        Helper.assertObjectNotNull(request::hasProjectCategoryId, "project_category_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasVersion, "version");
        Helper.assertObjectNotNull(request::hasMinScore, "min_score");
        Helper.assertObjectNotNull(request::hasMaxScore, "max_score");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateProjectCategoryIdProto(ProjectCategoryIdProto request) {
        Helper.assertObjectNotNull(request::hasProjectCategoryId, "project_category_id");
    }

    private void validateScorecardIdsProto(ScorecardIdsProto request) {
        Helper.assertObjectNotEmpty(request::getScorecardIdsCount, "scorecard_ids");
    }

    private void validateCreateSectionRequest(CreateSectionRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardGroupId, "scorecard_group_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasWeight, "weight");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
    }

    private void validateCreateSectionsRequest(CreateSectionsRequest request) {
        Helper.assertObjectNotEmpty(request::getSectionsCount, "sections");
        for (CreateSectionRequest createSectionRequest : request.getSectionsList()) {
            validateCreateSectionRequest(createSectionRequest);
        }
    }

    private void validateUpdateSectionRequest(UpdateSectionRequest request) {
        Helper.assertObjectNotNull(request::hasScorecardSectionId, "scorecard_section_id");
        Helper.assertObjectNotNull(request::hasScorecardGroupId, "scorecard_group_id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasWeight, "weight");
        Helper.assertObjectNotNull(request::hasSort, "sort");
        Helper.assertObjectNotNull(request::hasOperator, "operator");
    }

    private void validateSectionIdsProto(SectionIdsProto request) {
        Helper.assertObjectNotEmpty(request::getScorecardSectionIdsCount, "scorecard_section_ids");
    }
}
