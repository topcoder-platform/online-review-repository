package com.topcoder.or.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.util.SerializationUtils;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.grpc.upload.proto.*;
import com.topcoder.onlinereview.component.search.SearchBundle;
import com.topcoder.onlinereview.component.search.SearchBundleManager;
import com.topcoder.onlinereview.component.search.filter.Filter;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;
import com.topcoder.or.util.SearchBundleHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class UploadService extends UploadServiceGrpc.UploadServiceImplBase {
    private final DBAccessor dbAccessor;
    private final SearchBundleManager searchBundleManager;

    public static final String UPLOAD_SEARCH_BUNDLE_NAME = "Upload Search Bundle";
    public static final String SUBMISSION_SEARCH_BUNDLE_NAME = "Submission Search Bundle";

    private SearchBundle uploadSearchBundle;
    private SearchBundle submissionSearchBundle;

    public UploadService(DBAccessor dbAccessor, SearchBundleManager searchBundleManager) {
        this.dbAccessor = dbAccessor;
        this.searchBundleManager = searchBundleManager;
    }

    @PostConstruct
    public void postRun() {
        uploadSearchBundle = searchBundleManager.getSearchBundle(UPLOAD_SEARCH_BUNDLE_NAME);
        submissionSearchBundle = searchBundleManager.getSearchBundle(SUBMISSION_SEARCH_BUNDLE_NAME);
        SearchBundleHelper.setSearchableFields(uploadSearchBundle, SearchBundleHelper.UPLOAD_SEARCH_BUNDLE);
        SearchBundleHelper.setSearchableFields(submissionSearchBundle, SearchBundleHelper.SUBMISSION_SEARCH_BUNDLE);
    }

    @Override
    public void addUploadType(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProto(request);
        String sql = """
                INSERT INTO upload_type_lu (upload_type_id, create_user, create_date, modify_user, modify_date, name, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, request.getId(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getName(), request.getDescription());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void addUploadStatus(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProto(request);
        String sql = """
                INSERT INTO upload_status_lu (upload_status_id, create_user, create_date, modify_user, modify_date, name, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, request.getId(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getName(), request.getDescription());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void addSubmissionType(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProto(request);
        String sql = """
                INSERT INTO submission_type_lu (submission_type_id, create_user, create_date, modify_user, modify_date, name, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, request.getId(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getName(), request.getDescription());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void addSubmissionStatus(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProto(request);
        String sql = """
                INSERT INTO submission_status_lu (submission_status_id, create_user, create_date, modify_user, modify_date, name, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        dbAccessor.executeUpdate(sql, request.getId(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getName(), request.getDescription());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void addUpload(UploadProto request, StreamObserver<Empty> responseObserver) {
        validateUploadProto(request);
        String sql = """
                INSERT INTO upload (upload_id, create_user, create_date, modify_user, modify_date, project_id, project_phase_id,
                resource_id, upload_type_id, upload_status_id, parameter, upload_desc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final Long projecPhaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        final String uploadDesc = Helper.extract(request::hasUploadDesc, request::getUploadDesc);
        dbAccessor.executeUpdate(sql, request.getUploadId(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getProjectId(), projecPhaseId,
                request.getResourceId(), request.getUploadTypeId(), request.getUploadStatusId(), request.getParameter(),
                uploadDesc);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void addSubmission(SubmissionProto request, StreamObserver<Empty> responseObserver) {
        validateSubmissionProto(request);
        String sql = """
                INSERT INTO submission (submission_id, create_user, create_date, modify_user, modify_date, submission_status_id,
                submission_type_id, screening_score, initial_score, final_score, placement, user_rank, mark_for_purchase, prize_id, upload_id, thurgood_job_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final Double screeningScore = Helper.extract(request::hasScreeningScore, request::getScreeningScore);
        final Double initialScore = Helper.extract(request::hasInitialScore, request::getInitialScore);
        final Double finalScore = Helper.extract(request::hasFinalScore, request::getFinalScore);
        final Long placement = Helper.extract(request::hasPlacement, request::getPlacement);
        final Integer userRank = Helper.extract(request::hasUserRank, request::getUserRank);
        final Boolean markForPurchase = Helper.extract(request::hasMarkForPurchase, request::getMarkForPurchase);
        final Long prizeId = Helper.extract(request::hasPrizeId, request::getPrizeId);
        final String thurgoodJobId = Helper.extract(request::hasThurgoodJobId, request::getThurgoodJobId);
        dbAccessor.executeUpdate(sql, request.getSubmissionId(), request.getCreateUser(),
                Helper.convertDate(request.getCreateDate()), request.getModifyUser(),
                Helper.convertDate(request.getModifyDate()), request.getSubmissionStatusId(),
                request.getSubmissionTypeId(), screeningScore, initialScore, finalScore, placement, userRank,
                markForPurchase, prizeId, request.getUploadId(), thurgoodJobId);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void addSubmissionImage(SubmissionImageProto request, StreamObserver<Empty> responseObserver) {
        validateSubmissionImageProto(request);
        String sql = """
                INSERT INTO submission_image(submission_id, image_id, sort_order, modify_date, create_date)
                VALUES (?, ?, ?, ?, ?)
                """;
        final Date modifyDate = Helper.extractDate(request::hasModifyDate, request::getModifyDate);
        final Date createDate = Helper.extractDate(request::hasCreateDate, request::getCreateDate);
        dbAccessor.executeUpdate(sql, request.getSubmissionId(), request.getImageId(), request.getSortOrder(),
                modifyDate, createDate);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeUploadType(IdProto request, StreamObserver<Empty> responseObserver) {
        validateIdProto(request);
        String sql = """
                DELETE FROM upload_type_lu WHERE upload_type_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeUploadStatus(IdProto request, StreamObserver<Empty> responseObserver) {
        validateIdProto(request);
        String sql = """
                DELETE FROM upload_status_lu WHERE upload_status_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeSubmissionType(IdProto request, StreamObserver<Empty> responseObserver) {
        validateIdProto(request);
        String sql = """
                DELETE FROM submission_type_lu WHERE submission_type_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeSubmissionStatus(IdProto request, StreamObserver<Empty> responseObserver) {
        validateIdProto(request);
        String sql = """
                DELETE FROM submission_status_lu WHERE submission_status_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeUpload(IdProto request, StreamObserver<Empty> responseObserver) {
        validateIdProto(request);
        String sql = """
                DELETE FROM upload WHERE upload_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeSubmission(IdProto request, StreamObserver<Empty> responseObserver) {
        validateIdProto(request);
        String sql = """
                DELETE FROM submission WHERE submission_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeSubmissionImage(RemoveSubmissionImageRequest request, StreamObserver<Empty> responseObserver) {
        validateRemoveSubmissionImageRequest(request);
        String sql = """
                DELETE FROM submission_image WHERE submission_id = ? AND image_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getSubmissionId(), request.getImageId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateUploadType(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProtoForUpdate(request);
        String sql = """
                UPDATE upload_type_lu SET modify_user=?, modify_date=?, name=?, description=? WHERE upload_type_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getName(), request.getDescription(), request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateUploadStatus(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProtoForUpdate(request);
        String sql = """
                UPDATE upload_status_lu SET modify_user=?, modify_date=?, name=?, description=? WHERE upload_status_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getName(), request.getDescription(), request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateSubmissionType(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProtoForUpdate(request);
        String sql = """
                UPDATE submission_type_lu SET modify_user=?, modify_date=?, name=?, description=? WHERE submission_type_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getName(), request.getDescription(), request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateSubmissionStatus(EntityProto request, StreamObserver<Empty> responseObserver) {
        validateEntityProtoForUpdate(request);
        String sql = """
                UPDATE submission_status_lu SET modify_user=?, modify_date=?, name=?, description=? WHERE submission_status_id = ?
                """;
        dbAccessor.executeUpdate(sql, request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getName(), request.getDescription(), request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateUpload(UploadProto request, StreamObserver<Empty> responseObserver) {
        validateUploadProtoForUpdate(request);
        String sql = """
                UPDATE upload SET modify_user=?, modify_date=?, project_id=?, project_phase_id=?, resource_id=?, upload_type_id=?, upload_status_id=?, parameter=?, upload_desc=? WHERE upload_id=?
                """;
        final Long projecPhaseId = Helper.extract(request::hasProjectPhaseId, request::getProjectPhaseId);
        final String uploadDesc = Helper.extract(request::hasUploadDesc, request::getUploadDesc);
        dbAccessor.executeUpdate(sql, request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getProjectId(), projecPhaseId, request.getResourceId(), request.getUploadTypeId(),
                request.getUploadStatusId(), request.getParameter(), uploadDesc, request.getUploadId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateSubmission(SubmissionProto request, StreamObserver<Empty> responseObserver) {
        validateSubmissionProtoForUpdate(request);
        String sql = """
                UPDATE submission SET modify_user = ?, modify_date = ?, submission_status_id = ?, submission_type_id = ?,
                screening_score = ?, initial_score = ?, final_score = ?, placement = ?, user_rank = ?, mark_for_purchase = ?,
                prize_id = ?, upload_id = ?, thurgood_job_id = ? WHERE submission_id = ?
                """;
        final Double screeningScore = Helper.extract(request::hasScreeningScore, request::getScreeningScore);
        final Double initialScore = Helper.extract(request::hasInitialScore, request::getInitialScore);
        final Double finalScore = Helper.extract(request::hasFinalScore, request::getFinalScore);
        final Long placement = Helper.extract(request::hasPlacement, request::getPlacement);
        final Integer userRank = Helper.extract(request::hasUserRank, request::getUserRank);
        final Boolean markForPurchase = Helper.extract(request::hasMarkForPurchase, request::getMarkForPurchase);
        final Long prizeId = Helper.extract(request::hasPrizeId, request::getPrizeId);
        final String thurgoodJobId = Helper.extract(request::hasThurgoodJobId, request::getThurgoodJobId);
        dbAccessor.executeUpdate(sql, request.getModifyUser(), Helper.convertDate(request.getModifyDate()),
                request.getSubmissionStatusId(), request.getSubmissionTypeId(), screeningScore, initialScore,
                finalScore, placement, userRank, markForPurchase, prizeId, request.getUploadId(), thurgoodJobId,
                request.getSubmissionId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void updateSubmissionImage(SubmissionImageProto request, StreamObserver<Empty> responseObserver) {
        validateSubmissionImageProto(request);
        String sql = """
                UPDATE submission_image SET sort_order = ?, modify_date = ?, create_date = ? WHERE submission_id = ? AND image_id = ?
                """;
        final Date modifyDate = Helper.extractDate(request::hasModifyDate, request::getModifyDate);
        final Date createDate = Helper.extractDate(request::hasCreateDate, request::getCreateDate);
        dbAccessor.executeUpdate(sql, request.getSubmissionId(), request.getImageId(), request.getSortOrder(),
                modifyDate, createDate);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllUploadTypeIds(Empty request, StreamObserver<EntityIdsProto> responseObserver) {
        String sql = """
                SELECT upload_type_id FROM upload_type_lu
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        });
        responseObserver.onNext(EntityIdsProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllUploadStatusIds(Empty request, StreamObserver<EntityIdsProto> responseObserver) {
        String sql = """
                SELECT upload_status_id FROM upload_status_lu
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        });
        responseObserver.onNext(EntityIdsProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllSubmissionTypeIds(Empty request, StreamObserver<EntityIdsProto> responseObserver) {
        String sql = """
                SELECT submission_type_id FROM submission_type_lu
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        });
        responseObserver.onNext(EntityIdsProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllSubmissionStatusIds(Empty request, StreamObserver<EntityIdsProto> responseObserver) {
        String sql = """
                SELECT submission_status_id FROM submission_status_lu
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        });
        responseObserver.onNext(EntityIdsProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllMimeTypeIds(Empty request, StreamObserver<EntityIdsProto> responseObserver) {
        String sql = """
                SELECT mime_type_id FROM mime_type_lu
                """;
        List<Long> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            return rs.getLong(1);
        });
        responseObserver.onNext(EntityIdsProto.newBuilder().addAllIds(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadUploadTypes(EntityIdsProto request, StreamObserver<EntityListProto> responseObserver) {
        validateEntityIdsProto(request);
        String sql = """
                SELECT upload_type_id, create_user, create_date, modify_user, modify_date, name, description
                FROM upload_type_lu
                WHERE upload_type_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getIdsCount());
        List<EntityProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return loadEntityProto(rs);
        }, request.getIdsList().toArray());
        responseObserver.onNext(EntityListProto.newBuilder().addAllEntities(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadUploadStatuses(EntityIdsProto request, StreamObserver<EntityListProto> responseObserver) {
        validateEntityIdsProto(request);
        String sql = """
                SELECT upload_status_id, create_user, create_date, modify_user, modify_date, name, description
                FROM upload_status_lu
                WHERE upload_status_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getIdsCount());
        List<EntityProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return loadEntityProto(rs);
        }, request.getIdsList().toArray());
        responseObserver.onNext(EntityListProto.newBuilder().addAllEntities(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadSubmissionTypes(EntityIdsProto request, StreamObserver<EntityListProto> responseObserver) {
        validateEntityIdsProto(request);
        String sql = """
                SELECT submission_type_id, create_user, create_date, modify_user, modify_date, name, description
                FROM submission_type_lu
                WHERE submission_type_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getIdsCount());
        List<EntityProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return loadEntityProto(rs);
        }, request.getIdsList().toArray());
        responseObserver.onNext(EntityListProto.newBuilder().addAllEntities(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadSubmissionStatuses(EntityIdsProto request, StreamObserver<EntityListProto> responseObserver) {
        validateEntityIdsProto(request);
        String sql = """
                SELECT submission_status_id, create_user, create_date, modify_user, modify_date, name, description
                FROM submission_status_lu
                WHERE submission_status_id IN (%s)
                """;
        String inSql = Helper.getInClause(request.getIdsCount());
        List<EntityProto> result = dbAccessor.executeQuery(sql.formatted(inSql), (rs, _i) -> {
            return loadEntityProto(rs);
        }, request.getIdsList().toArray());
        responseObserver.onNext(EntityListProto.newBuilder().addAllEntities(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadUploads(EntityIdsProto request, StreamObserver<UploadCompleteListProto> responseObserver) {
        validateEntityIdsProto(request);
        String sql = """
                SELECT upload.upload_id, upload.create_user as upload_create_user, upload.create_date as upload_create_date,
                upload.modify_user, upload.modify_date as upload_modify_user, upload.project_id, upload.project_phase_id,
                upload.resource_id, upload.parameter as upload_parameter, upload.upload_desc, upload.url, upload_type_lu.upload_type_id,
                upload_type_lu.create_user as upload_type_create_user, upload_type_lu.create_date as upload_type_create_date,
                upload_type_lu.modify_user as upload_type_modify_user, upload_type_lu.modify_date as upload_type_modify_date,
                upload_type_lu.name as upload_type_name, upload_type_lu.description as upload_type_description, upload_status_lu.upload_status_id,
                upload_status_lu.create_user as upload_status_create_user, upload_status_lu.create_date as upload_status_create_date,
                upload_status_lu.modify_user as upload_status_modify_user, upload_status_lu.modify_date as upload_status_modify_date,
                upload_status_lu.name as upload_status_name, upload_status_lu.description as upload_status_description
                FROM upload
                INNER JOIN upload_type_lu ON upload.upload_type_id=upload_type_lu.upload_type_id
                INNER JOIN upload_status_lu ON upload.upload_status_id=upload_status_lu.upload_status_id
                WHERE upload_id IN (%s)
                """;
        String inSql = " (%s)".formatted(Helper.getInClause(request.getIdsCount()));
        List<UploadCompleteProto> result = dbAccessor.executeQuery(sql.concat(inSql), (rs, _i) -> {
            return loadUpload(rs);
        }, request.getIdsList().toArray());
        responseObserver.onNext(UploadCompleteListProto.newBuilder().addAllUploads(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadSubmissions(EntityIdsProto request, StreamObserver<SubmissionCompleteListProto> responseObserver) {
        validateEntityIdsProto(request);
        String sql = """
                SELECT submission.submission_id, submission.create_user as submission_create_user, submission.create_date as submission_create_date,
                submission.modify_user as submission_modify_user, submission.modify_date as submission_modify_date,
                submission_status_lu.submission_status_id, submission_status_lu.create_user as submission_status_create_user,
                submission_status_lu.create_date as submission_status_create_date, submission_status_lu.modify_user as submission_status_modify_user,
                submission_status_lu.modify_date as submission_status_modify_date, submission_status_lu.name as submission_status_name,
                submission_status_lu.description as submission_status_description, submission_type_lu.submission_type_id,
                submission_type_lu.create_user as submission_type_create_user, submission_type_lu.create_date as submission_type_create_date,
                submission_type_lu.modify_user as submission_type_modify_user, submission_type_lu.modify_date as submission_type_modify_date,
                submission_type_lu.name as submission_type_name, submission_type_lu.description as submission_type_description,
                submission.screening_score, submission.initial_score, submission.final_score, submission.placement, submission.user_rank,
                submission.mark_for_purchase, submission.thurgood_job_id, prize.prize_id, prize.place, prize.prize_amount, prize.prize_type_id,
                prize.number_of_submissions, prize.create_user as prize_create_user, prize.create_date as prize_create_date,
                prize.modify_user as prize_modify_user, prize.modify_date as prize_modify_date, prize_type_lu.prize_type_desc,
                upload.upload_id, upload.create_user as upload_create_user, upload.create_date as upload_create_date,
                upload.modify_user as upload_modify_user, upload.modify_date as upload_modify_date, upload.project_id, upload.project_phase_id,
                upload.resource_id, upload.parameter as upload_parameter, upload.upload_desc, upload_type_lu.upload_type_id,
                upload_type_lu.create_user as upload_type_create_user, upload_type_lu.create_date as upload_type_create_date,
                upload_type_lu.modify_user as upload_type_modify_user, upload_type_lu.modify_date as upload_type_modify_date,
                upload_type_lu.name as upload_type_name, upload_type_lu.description as upload_type_description, upload_status_lu.upload_status_id,
                upload_status_lu.create_user as upload_status_create_user, upload_status_lu.create_date as upload_status_create_date,
                upload_status_lu.modify_user as upload_status_modify_user, upload_status_lu.modify_date as upload_status_modify_date,
                upload_status_lu.name as upload_status_name, upload_status_lu.description as upload_status_description, upload.url
                FROM submission
                INNER JOIN submission_status_lu ON submission.submission_status_id = submission_status_lu.submission_status_id
                INNER JOIN submission_type_lu ON submission.submission_type_id = submission_type_lu.submission_type_id
                LEFT JOIN prize ON submission.prize_id = prize.prize_id
                LEFT JOIN prize_type_lu ON prize.prize_type_id = prize_type_lu.prize_type_id
                INNER JOIN upload ON submission.upload_id = upload.upload_id
                INNER JOIN upload_type_lu ON upload.upload_type_id=upload_type_lu.upload_type_id
                INNER JOIN upload_status_lu ON upload.upload_status_id=upload_status_lu.upload_status_id
                WHERE submission.submission_id IN
                """;
        String inSql = " (%s)".formatted(Helper.getInClause(request.getIdsCount()));
        List<SubmissionCompleteProto> result = dbAccessor.executeQuery(sql.concat(inSql), (rs, _i) -> {
            return loadSubmission(rs);
        }, request.getIdsList().toArray());
        responseObserver.onNext(SubmissionCompleteListProto.newBuilder().addAllSubmissions(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadMimeTypes(EntityIdsProto request, StreamObserver<MimeTypeListProto> responseObserver) {
        validateEntityIdsProto(request);
        String sql = """
                SELECT mime_type_id, file_type_lu.file_type_id,file_type_lu.file_type_desc, file_type_lu.sort,
                file_type_lu.image_file_ind, file_type_lu.extension,file_type_lu.bundled_file_ind, mime_type_desc
                FROM mime_type_lu
                INNER JOIN file_type_lu ON mime_type_lu.file_type_id = file_type_lu.file_type_id
                WHERE mime_type_id IN
                """;
        String inSql = " (%s)".formatted(Helper.getInClause(request.getIdsCount()));
        List<MimeTypeProto> result = dbAccessor.executeQuery(sql.concat(inSql), (rs, _i) -> {
            MimeTypeProto.Builder builder = MimeTypeProto.newBuilder();
            ResultSetHelper.applyResultSetLong(rs, 1, builder::setMimeTypeId);
            ResultSetHelper.applyResultSetLong(rs, 2, builder::setFileTypeId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setFileTypeDesc);
            ResultSetHelper.applyResultSetInt(rs, 4, builder::setSort);
            ResultSetHelper.applyResultSetBool(rs, 5, builder::setImageFileInd);
            ResultSetHelper.applyResultSetString(rs, 6, builder::setExtension);
            ResultSetHelper.applyResultSetBool(rs, 7, builder::setBundledFileInd);
            ResultSetHelper.applyResultSetString(rs, 8, builder::setMimeTypeDesc);
            return builder.build();
        }, request.getIdsList().toArray());
        responseObserver.onNext(MimeTypeListProto.newBuilder().addAllMimeTypes(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getImagesForSubmission(IdProto request, StreamObserver<SubmissionImageListProto> responseObserver) {
        String sql = """
                SELECT image_id, sort_order, modify_date, create_date
                FROM submission_image
                WHERE submission_id = ?
                """;
        List<SubmissionImageProto> result = dbAccessor.executeQuery(sql, (rs, _i) -> {
            SubmissionImageProto.Builder builder = SubmissionImageProto.newBuilder();
            ResultSetHelper.applyResultSetInt(rs, 1, builder::setImageId);
            ResultSetHelper.applyResultSetInt(rs, 2, builder::setSortOrder);
            ResultSetHelper.applyResultSetTimestamp(rs, 3, builder::setModifyDate);
            ResultSetHelper.applyResultSetTimestamp(rs, 4, builder::setCreateDate);
            return builder.build();
        }, request.getId());
        responseObserver.onNext(SubmissionImageListProto.newBuilder().addAllSubmissionImages(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchUploads(FilterProto request, StreamObserver<UploadCompleteListProto> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<UploadCompleteProto> result = uploadSearchBundle.search(filter, (rs, _i) -> {
            return loadUpload(rs);
        });
        responseObserver.onNext(UploadCompleteListProto.newBuilder().addAllUploads(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchSubmissions(FilterProto request, StreamObserver<SubmissionCompleteListProto> responseObserver) {
        Filter filter = (Filter) SerializationUtils.deserialize(request.getFilter().toByteArray());
        List<SubmissionCompleteProto> result = submissionSearchBundle.search(filter, (rs, _i) -> {
            return loadSubmission(rs);
        });
        responseObserver.onNext(SubmissionCompleteListProto.newBuilder().addAllSubmissions(result).build());
        responseObserver.onCompleted();
    }

    private void validateEntityProto(EntityProto request) {
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        validateEntityProtoForUpdate(request);
    }

    private void validateEntityProtoForUpdate(EntityProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasId, request::getId, "id");
        Helper.assertObjectNotNull(request::hasName, "name");
        Helper.assertObjectNotNull(request::hasDescription, "description");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateUploadProto(UploadProto request) {
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        validateUploadProtoForUpdate(request);
    }

    private void validateUploadProtoForUpdate(UploadProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasUploadId, request::getUploadId, "upload_id");
        Helper.assertObjectNotNullAndPositive(request::hasUploadTypeId, request::getUploadTypeId, "upload_type_id");
        Helper.assertObjectNotNullAndPositive(request::hasUploadStatusId, request::getUploadStatusId,
                "upload_status_id");
        Helper.assertObjectNotNullAndPositive(request::hasResourceId, request::getResourceId, "resource_id");
        Helper.assertObjectNotNullAndPositive(request::hasProjectId, request::getProjectId, "project_id");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
        Helper.assertObjectNotNull(request::hasParameter, "parameter");
    }

    private void validateSubmissionProto(SubmissionProto request) {
        Helper.assertObjectNotNull(request::hasCreateUser, "create_user");
        Helper.assertObjectNotNull(request::hasCreateDate, "create_date");
        validateSubmissionProtoForUpdate(request);
    }

    private void validateSubmissionProtoForUpdate(SubmissionProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasSubmissionId, request::getSubmissionId, "submission_id");
        Helper.assertObjectNotNullAndPositive(request::hasUploadId, request::getUploadId, "upload_id");
        Helper.assertObjectNotNullAndPositive(request::hasSubmissionStatusId, request::getSubmissionStatusId,
                "submission_status_id");
        Helper.assertObjectNotNullAndPositive(request::hasSubmissionTypeId, request::getSubmissionTypeId,
                "submission_type_id");
        Helper.assertObjectNotNull(request::hasModifyUser, "modify_user");
        Helper.assertObjectNotNull(request::hasModifyDate, "modify_date");
    }

    private void validateSubmissionImageProto(SubmissionImageProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasSubmissionId, request::getSubmissionId, "submission_id");
        Helper.assertObjectNotNullAndPositive(request::hasImageId, request::getImageId, "image_id");
        Helper.assertObjectNotNull(request::hasSortOrder, "sort_order");
    }

    private void validateIdProto(IdProto request) {
        Helper.assertObjectNotNullAndPositive(request::hasId, request::getId, "id");
    }

    private void validateRemoveSubmissionImageRequest(RemoveSubmissionImageRequest request) {
        Helper.assertObjectNotNullAndPositive(request::hasSubmissionId, request::getSubmissionId, "submission_id");
        Helper.assertObjectNotNullAndPositive(request::hasImageId, request::getImageId, "image_id");
    }

    private void validateEntityIdsProto(EntityIdsProto request) {
        Helper.assertObjectNotEmpty(request::getIdsCount, "id");
    }

    private EntityProto loadEntityProto(ResultSet rs) throws SQLException {
        EntityProto.Builder builder = EntityProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, 1, builder::setId);
        ResultSetHelper.applyResultSetString(rs, 2, builder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, 3, builder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, 4, builder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, 5, builder::setModifyDate);
        ResultSetHelper.applyResultSetString(rs, 6, builder::setName);
        ResultSetHelper.applyResultSetString(rs, 7, builder::setDescription);
        return builder.build();
    }

    private UploadCompleteProto loadUpload(ResultSet rs) throws SQLException {
        UploadCompleteProto.Builder builder = UploadCompleteProto.newBuilder();
        UploadProto.Builder uBuilder = UploadProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "upload_id", uBuilder::setUploadId);
        ResultSetHelper.applyResultSetString(rs, "upload_create_user", uBuilder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "upload_create_date", uBuilder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "upload_modify_user", uBuilder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "upload_modify_date", uBuilder::setModifyDate);
        ResultSetHelper.applyResultSetLong(rs, "project_id", uBuilder::setProjectId);
        ResultSetHelper.applyResultSetLong(rs, "project_phase_id", uBuilder::setProjectPhaseId);
        ResultSetHelper.applyResultSetLong(rs, "resource_id", uBuilder::setResourceId);
        ResultSetHelper.applyResultSetString(rs, "upload_parameter", uBuilder::setParameter);
        ResultSetHelper.applyResultSetString(rs, "upload_desc", uBuilder::setUploadDesc);
        ResultSetHelper.applyResultSetString(rs, "url", uBuilder::setUrl);
        builder.setUpload(uBuilder.build());
        EntityProto.Builder utBuilder = EntityProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "upload_type_id", utBuilder::setId);
        ResultSetHelper.applyResultSetString(rs, "upload_type_create_user", utBuilder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "upload_type_create_date", utBuilder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "upload_type_modify_user", utBuilder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "upload_type_modify_date", utBuilder::setModifyDate);
        ResultSetHelper.applyResultSetString(rs, "upload_type_name", utBuilder::setName);
        ResultSetHelper.applyResultSetString(rs, "upload_type_description", utBuilder::setDescription);
        builder.setUploadType(utBuilder.build());
        EntityProto.Builder usBuilder = EntityProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "upload_status_id", usBuilder::setId);
        ResultSetHelper.applyResultSetString(rs, "upload_status_create_user", usBuilder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "upload_status_create_date", usBuilder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "upload_status_modify_user", usBuilder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "upload_status_modify_date", usBuilder::setModifyDate);
        ResultSetHelper.applyResultSetString(rs, "upload_status_name", usBuilder::setName);
        ResultSetHelper.applyResultSetString(rs, "upload_status_description", usBuilder::setDescription);
        builder.setUploadStatus(usBuilder.build());
        return builder.build();
    }

    private SubmissionCompleteProto loadSubmission(ResultSet rs) throws SQLException {
        SubmissionCompleteProto.Builder builder = SubmissionCompleteProto.newBuilder();
        SubmissionProto.Builder sBuilder = SubmissionProto.newBuilder();
        ResultSetHelper.applyResultSetDouble(rs, "screening_score", sBuilder::setScreeningScore);
        ResultSetHelper.applyResultSetDouble(rs, "initial_score", sBuilder::setInitialScore);
        ResultSetHelper.applyResultSetDouble(rs, "final_score", sBuilder::setFinalScore);
        ResultSetHelper.applyResultSetLong(rs, "placement", sBuilder::setPlacement);
        ResultSetHelper.applyResultSetBool(rs, "mark_for_purchase", sBuilder::setMarkForPurchase);
        ResultSetHelper.applyResultSetString(rs, "thurgood_job_id", sBuilder::setThurgoodJobId);
        ResultSetHelper.applyResultSetLong(rs, "submission_id", sBuilder::setSubmissionId);
        ResultSetHelper.applyResultSetInt(rs, "user_rank", sBuilder::setUserRank);
        ResultSetHelper.applyResultSetString(rs, "submission_create_user", sBuilder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "submission_create_date", sBuilder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "submission_modify_user", sBuilder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "submission_modify_date", sBuilder::setModifyDate);
        builder.setSubmission(sBuilder.build());
        EntityProto.Builder ssBuilder = EntityProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "submission_status_id", ssBuilder::setId);
        ResultSetHelper.applyResultSetString(rs, "submission_status_create_user", ssBuilder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "submission_status_create_date", ssBuilder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "submission_status_modify_user", ssBuilder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "submission_status_modify_date", ssBuilder::setModifyDate);
        ResultSetHelper.applyResultSetString(rs, "submission_status_name", ssBuilder::setName);
        ResultSetHelper.applyResultSetString(rs, "submission_status_description", ssBuilder::setDescription);
        builder.setSubmissionStatus(ssBuilder.build());
        EntityProto.Builder stBuilder = EntityProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "submission_type_id", stBuilder::setId);
        ResultSetHelper.applyResultSetString(rs, "submission_type_create_user", stBuilder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "submission_type_create_date", stBuilder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "submission_type_modify_user", stBuilder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "submission_type_modify_date", stBuilder::setModifyDate);
        ResultSetHelper.applyResultSetString(rs, "submission_type_name", stBuilder::setName);
        ResultSetHelper.applyResultSetString(rs, "submission_type_description", stBuilder::setDescription);
        builder.setSubmissionType(stBuilder.build());
        PrizeProto.Builder pBuilder = PrizeProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "prize_id", pBuilder::setPrizeId);
        ResultSetHelper.applyResultSetInt(rs, "place", pBuilder::setPlace);
        ResultSetHelper.applyResultSetDouble(rs, "prize_amount", pBuilder::setPrizeAmount);
        ResultSetHelper.applyResultSetInt(rs, "number_of_submissions", pBuilder::setNumberOfSubmissions);
        ResultSetHelper.applyResultSetString(rs, "prize_create_user", pBuilder::setCreateUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "prize_create_date", pBuilder::setCreateDate);
        ResultSetHelper.applyResultSetString(rs, "prize_modify_user", pBuilder::setModifyUser);
        ResultSetHelper.applyResultSetTimestamp(rs, "prize_modify_date", pBuilder::setModifyDate);
        builder.setPrize(pBuilder.build());
        PrizeTypeProto.Builder ptBuilder = PrizeTypeProto.newBuilder();
        ResultSetHelper.applyResultSetLong(rs, "prize_type_id", ptBuilder::setPrizeTypeId);
        ResultSetHelper.applyResultSetString(rs, "prize_type_desc", ptBuilder::setPrizeTypeDesc);
        builder.setPrizeType(ptBuilder.build());
        builder.setUpload(loadUpload(rs));
        return builder.build();
    }
}