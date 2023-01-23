package com.topcoder.or.repository;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.protobuf.Empty;
import com.topcoder.onlinereview.component.shared.dataaccess.DataAccess;
import com.topcoder.onlinereview.component.shared.dataaccess.Request;
import com.topcoder.onlinereview.grpc.webcommon.proto.*;
import com.topcoder.or.util.DBAccessor;
import com.topcoder.or.util.Helper;
import com.topcoder.or.util.ResultSetHelper;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class WebCommonService extends WebCommonServiceGrpc.WebCommonServiceImplBase {
    private final DBAccessor dbAccessor;

    public WebCommonService(DBAccessor dbAccessor) {
        this.dbAccessor = dbAccessor;
    }

    @Override
    public void getUserPassword(GetUserPasswordRequest request,
            StreamObserver<GetUserPasswordResponse> responseObserver) {
        validateGetUserPasswordRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getOltpJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "userid_to_password";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> userData = result.get(queryName);
        GetUserPasswordResponse response;
        if (userData.isEmpty()) {
            response = GetUserPasswordResponse.getDefaultInstance();
        } else {
            GetUserPasswordResponse.Builder builder = GetUserPasswordResponse.newBuilder();
            String password = Helper.getString(userData.get(0), "password");
            if (password != null) {
                builder.setPassword(password);
            }
            String status = Helper.getString(userData.get(0), "status");
            if (status != null) {
                builder.setStatus(status);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getUserTimezone(GetUserTimezoneRequest request,
            StreamObserver<GetUserTimezoneResponse> responseObserver) {
        validateGetUserTimezoneRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getOltpJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "user_timezone";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("uid", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> userData = result.get(queryName);
        GetUserTimezoneResponse response;
        if (userData.isEmpty()) {
            response = GetUserTimezoneResponse.getDefaultInstance();
        } else {
            GetUserTimezoneResponse.Builder builder = GetUserTimezoneResponse.newBuilder();
            String timeZone = Helper.getString(userData.get(0), "timezone_desc");
            if (timeZone != null) {
                builder.setTimezoneDesc(timeZone);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getMemberCount(Empty request, StreamObserver<GetMemberCountResponse> responseObserver) {
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getOltpJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "member_count";
        dbRequest.setContentHandle(queryName);
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> userData = result.get(queryName);
        GetMemberCountResponse response;
        if (userData.isEmpty()) {
            response = GetMemberCountResponse.getDefaultInstance();
        } else {
            GetMemberCountResponse.Builder builder = GetMemberCountResponse.newBuilder();
            Integer memberCount = Helper.getInt(userData.get(0), "member_count");
            if (memberCount != null) {
                builder.setMemberCount(memberCount);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getMemberImage(GetMemberImageRequest request, StreamObserver<GetMemberImageResponse> responseObserver) {
        validateGetMemberImageRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getOltpJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "member_image";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("cr", String.valueOf(request.getUserId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> userData = result.get("coder_image_data");
        GetMemberImageResponse response;
        if (userData.isEmpty()) {
            response = GetMemberImageResponse.getDefaultInstance();
        } else {
            GetMemberImageResponse.Builder builder = GetMemberImageResponse.newBuilder();
            Integer imageId = Helper.getInt(userData.get(0), "image_id");
            if (imageId != null) {
                builder.setImageId(imageId);
            }
            String imagePath = Helper.getString(userData.get(0), "image_path");
            if (imagePath != null) {
                builder.setImagePath(imagePath);
            }
            String fileName = Helper.getString(userData.get(0), "file_name");
            if (fileName != null) {
                builder.setFileName(fileName);
            }
            response = builder.build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getCoderAllRatings(GetCoderAllRatingsRequest request,
            StreamObserver<GetCoderAllRatingsResponse> responseObserver) {
        validateGetCoderAllRatingsRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getOltpJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = "coder_all_ratings";
        dbRequest.setContentHandle(queryName);
        dbRequest.setProperty("cr", String.valueOf(request.getCoderId()));
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> userData = result.get(queryName);
        GetCoderAllRatingsResponse response;
        if (userData.isEmpty()) {
            response = GetCoderAllRatingsResponse.getDefaultInstance();
        } else {
            Map<String, Object> data = userData.get(0);
            CoderRatingsProto.Builder builder = CoderRatingsProto.newBuilder();
            Long coderId = Helper.getLong(data, "coder_id");
            if (coderId != null) {
                builder.setCoderId(coderId);
            }
            String handle = Helper.getString(data, "handle");
            if (handle != null) {
                builder.setHandle(handle);
            }
            Integer algorithmRating = Helper.getInt(data, "algorithm_rating");
            if (algorithmRating != null) {
                builder.setAlgorithmRating(algorithmRating);
            }
            Integer hsAlgorithmRating = Helper.getInt(data, "hs_algorithm_rating");
            if (hsAlgorithmRating != null) {
                builder.setAlgorithmRating(hsAlgorithmRating);
            }
            Integer marathonMatchRating = Helper.getInt(data, "marathon_match_rating");
            if (marathonMatchRating != null) {
                builder.setMarathonMatchRating(marathonMatchRating);
            }
            Integer designRating = Helper.getInt(data, "design_rating");
            if (designRating != null) {
                builder.setDesignRating(designRating);
            }
            Integer developmentRating = Helper.getInt(data, "development_rating");
            if (developmentRating != null) {
                builder.setDevelopmentRating(developmentRating);
            }
            Integer conceptualizationRating = Helper.getInt(data, "conceptualization_rating");
            if (conceptualizationRating != null) {
                builder.setConceptualizationRating(conceptualizationRating);
            }
            Integer specificationRating = Helper.getInt(data, "specification_rating");
            if (specificationRating != null) {
                builder.setSpecificationRating(specificationRating);
            }
            Integer architectureRating = Helper.getInt(data, "architecture_rating");
            if (architectureRating != null) {
                builder.setArchitectureRating(architectureRating);
            }
            Integer assemblyRating = Helper.getInt(data, "assembly_rating");
            if (assemblyRating != null) {
                builder.setAssemblyRating(assemblyRating);
            }
            Integer testSuitesRating = Helper.getInt(data, "test_suites_rating");
            if (testSuitesRating != null) {
                builder.setTestSuitesRating(testSuitesRating);
            }
            Integer testScenariosRating = Helper.getInt(data, "test_scenarios_rating");
            if (testScenariosRating != null) {
                builder.setTestScenariosRating(testScenariosRating);
            }
            Integer uiPrototypeRating = Helper.getInt(data, "ui_prototype_rating");
            if (uiPrototypeRating != null) {
                builder.setUiPrototypeRating(uiPrototypeRating);
            }
            Integer riaBuildRating = Helper.getInt(data, "ria_build_rating");
            if (riaBuildRating != null) {
                builder.setRiaBuildRating(riaBuildRating);
            }
            Integer contentCreationRating = Helper.getInt(data, "content_creation_rating");
            if (contentCreationRating != null) {
                builder.setContentCreationRating(contentCreationRating);
            }
            Integer reportingRating = Helper.getInt(data, "reporting_rating");
            if (reportingRating != null) {
                builder.setReportingRating(reportingRating);
            }
            response = GetCoderAllRatingsResponse.newBuilder().setCoderRatings(builder.build()).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void doStartTag(DoStartTagRequest request, StreamObserver<DoStartTagResponse> responseObserver) {
        validateDoStartTagRequest(request);
        DataAccess dataAccess = new DataAccess(dbAccessor, dbAccessor.getOltpJdbcTemplate());
        Request dbRequest = new Request();
        String queryName = request.getCommand();
        dbRequest.setContentHandle(queryName);
        for (ParameterProto parameter : request.getParametersList()) {
            dbRequest.setProperty(parameter.getKey(), parameter.getValue());
        }
        Map<String, List<Map<String, Object>>> result = dataAccess.getData(dbRequest);
        List<Map<String, Object>> userData = result.get(queryName);
        DoStartTagResponse.Builder response = DoStartTagResponse.newBuilder();
        if (userData != null) {
            for (Map<String, Object> row : userData) {
                ParameterListProto.Builder builder = ParameterListProto.newBuilder();
                for (Entry<String, Object> entry : row.entrySet()) {
                    ParameterProto.Builder pBuilder = ParameterProto.newBuilder();
                    pBuilder.setKey(entry.getKey());
                    if (entry.getValue() != null) {
                        pBuilder.setValue(entry.getValue().toString());
                    }
                    builder.addParameters(pBuilder.build());
                }
                response.addParameterLists(builder.build());
            }
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDataTypeMappings(Empty request, StreamObserver<GetDataTypeMappingsResponse> responseObserver) {
        String sql = """
                SELECT data_type_id, language_id, display_value
                FROM data_type_mapping
                """;
        List<DataTypeMappingProto> result = dbAccessor.executeQuery(dbAccessor.getOltpJdbcTemplate(), sql, (rs, _i) -> {
            DataTypeMappingProto.Builder builder = DataTypeMappingProto.newBuilder();
            ResultSetHelper.applyResultSetString(rs, 1, builder::setDataTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setLanguageId);
            ResultSetHelper.applyResultSetString(rs, 3, builder::setDisplayValue);
            return builder.build();
        });
        responseObserver.onNext(GetDataTypeMappingsResponse.newBuilder().addAllDataTypeMappings(result).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDataTypes(Empty request, StreamObserver<GetDataTypesResponse> responseObserver) {
        String sql = """
                SELECT data_type_id, data_type_desc
                FROM data_type
                """;
        List<DataTypeProto> result = dbAccessor.executeQuery(dbAccessor.getOltpJdbcTemplate(), sql, (rs, _i) -> {
            DataTypeProto.Builder builder = DataTypeProto.newBuilder();
            ResultSetHelper.applyResultSetInt(rs, 1, builder::setDataTypeId);
            ResultSetHelper.applyResultSetString(rs, 2, builder::setDataTypeDesc);
            return builder.build();
        });
        responseObserver.onNext(GetDataTypesResponse.newBuilder().addAllDataTypes(result).build());
        responseObserver.onCompleted();
    }

    private void validateGetUserPasswordRequest(GetUserPasswordRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetUserTimezoneRequest(GetUserTimezoneRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetMemberImageRequest(GetMemberImageRequest request) {
        Helper.assertObjectNotNull(request::hasUserId, "user_id");
    }

    private void validateGetCoderAllRatingsRequest(GetCoderAllRatingsRequest request) {
        Helper.assertObjectNotNull(request::hasCoderId, "coder_id");
    }

    private void validateDoStartTagRequest(DoStartTagRequest request) {
        Helper.assertObjectNotNull(request::hasCommand, "command");
    }
}
