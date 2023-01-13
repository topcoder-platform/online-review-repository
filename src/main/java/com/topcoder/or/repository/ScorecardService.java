package com.topcoder.or.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.rpc.Help;
import com.topcoder.onlinereview.component.id.DBHelper;
import com.topcoder.onlinereview.component.id.IDGenerator;
import com.topcoder.onlinereview.grpc.scorecard.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class ScorecardService extends ScorecardServiceGrpc.ScorecardServiceImplBase {
    private final DBAccessor dbAccessor;
    private final DBHelper dbHelper;

    private IDGenerator groupIdGenerator;
    private IDGenerator questionIdGenerator;
    private IDGenerator sectionIdGenerator;

    private static final String SCORECARD_GROUP_ID_SEQUENCE = "scorecard_group_id_seq";
    private static final String SCORECARD_QUESTION_ID_SEQUENCE = "scorecard_question_id_seq";
    private static final String SCORECARD_SECTION_ID_SEQUENCE = "scorecard_section_id_seq";

    public ScorecardService(DBAccessor dbAccessor, DBHelper dbHelper) {
        this.dbAccessor = dbAccessor;
        this.dbHelper = dbHelper;
    }

    @PostConstruct
    public void postRun() {
        groupIdGenerator = new IDGenerator(SCORECARD_GROUP_ID_SEQUENCE, dbHelper);
        questionIdGenerator = new IDGenerator(SCORECARD_QUESTION_ID_SEQUENCE, dbHelper);
        sectionIdGenerator = new IDGenerator(SCORECARD_SECTION_ID_SEQUENCE, dbHelper);
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
