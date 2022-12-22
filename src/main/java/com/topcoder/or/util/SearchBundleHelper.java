package com.topcoder.or.util;

import java.util.HashMap;
import java.util.Map;

import com.topcoder.onlinereview.component.datavalidator.IntegerValidator;
import com.topcoder.onlinereview.component.datavalidator.LongValidator;
import com.topcoder.onlinereview.component.datavalidator.ObjectValidator;
import com.topcoder.onlinereview.component.datavalidator.StringValidator;
import com.topcoder.onlinereview.component.search.SearchBundle;

public final class SearchBundleHelper {

    public static final int DELIVERABLE_SEARCH_BUNDLE = 1;
    public static final int DELIVERABLE_WITH_SUBMISSIONS_SEARCH_BUNDLE = 2;
    public static final int UPLOAD_SEARCH_BUNDLE = 3;
    public static final int SUBMISSION_SEARCH_BUNDLE = 4;
    public static final int PROJECT_SEARCH_BUNDLE = 5;

    /**
     * Set the searchable fields of given SearchBundle.
     * <p>
     * <em>Changes in 1.1:</em>
     * <ul>
     * <li>Add the searchable field 'submission_type_id' of submission search
     * bundle.</li>
     * </ul>
     * </p>
     * <p>
     * Changes in version 1.2:
     * <ul>
     * <li>Changes to generic type support.</li>
     * </ul>
     * </p>
     *
     * @param searchBundle
     *                     the SearchBundle to set
     * @param key
     *                     the identifier of SearchBundle
     */
    public static void setSearchableFields(SearchBundle searchBundle, int key) {
        Map<String, ObjectValidator> fields = new HashMap<>();

        // Set up an IntegerValidator for latter use.
        IntegerValidator greaterThanZeroValidator = IntegerValidator.greaterThan(0);

        // Set the fields with different validator.
        switch (key) {
            case DELIVERABLE_WITH_SUBMISSIONS_SEARCH_BUNDLE:
                fields.put("project_id", greaterThanZeroValidator);
                fields.put("resource_id", greaterThanZeroValidator);
                fields.put("submission_id", greaterThanZeroValidator);
                fields.put("deliverable_id", greaterThanZeroValidator);
                fields.put("phase_id", greaterThanZeroValidator);
                fields.put("name", StringValidator.hasLength(greaterThanZeroValidator));
                fields.put("required", IntegerValidator.inRange(Integer.MIN_VALUE, Integer.MAX_VALUE));
                break;

            case DELIVERABLE_SEARCH_BUNDLE:
                fields.put("project_id", greaterThanZeroValidator);
                fields.put("resource_id", greaterThanZeroValidator);
                fields.put("deliverable_id", greaterThanZeroValidator);
                fields.put("phase_id", greaterThanZeroValidator);
                fields.put("name", StringValidator.hasLength(greaterThanZeroValidator));
                fields.put("required", IntegerValidator.inRange(Integer.MIN_VALUE, Integer.MAX_VALUE));
                break;

            case UPLOAD_SEARCH_BUNDLE:
                fields.put("project_id", greaterThanZeroValidator);
                fields.put("resource_id", greaterThanZeroValidator);
                fields.put("upload_id", greaterThanZeroValidator);
                fields.put("upload_type_id", greaterThanZeroValidator);
                fields.put("upload_status_id", greaterThanZeroValidator);
                fields.put("project_phase_id", greaterThanZeroValidator);
                break;

            case SUBMISSION_SEARCH_BUNDLE:
                fields.put("project_id", greaterThanZeroValidator);
                fields.put("resource_id", greaterThanZeroValidator);
                fields.put("upload_id", greaterThanZeroValidator);
                fields.put("submission_id", greaterThanZeroValidator);
                fields.put("submission_status_id", greaterThanZeroValidator);
                fields.put("submission_type_id", greaterThanZeroValidator);
                fields.put("project_phase_id", greaterThanZeroValidator);
                break;

            case PROJECT_SEARCH_BUNDLE:
                fields.put("ProjectTypeID", LongValidator.isPositive());
                fields.put("ProjectCategoryID", LongValidator.isPositive());
                fields.put("ProjectStatusID", LongValidator.isPositive());
                fields.put("ProjectTypeName", StringValidator.hasLength(IntegerValidator.lessThanOrEqualTo(64)));
                fields.put("ProjectCategoryName", StringValidator.hasLength(IntegerValidator.lessThanOrEqualTo(64)));
                fields.put("ProjectStatusName", StringValidator.hasLength(IntegerValidator.lessThanOrEqualTo(64)));
                fields.put("ProjectPropertyName", StringValidator.hasLength(IntegerValidator.lessThanOrEqualTo(64)));
                fields.put("ProjectPropertyValue", StringValidator.hasLength(IntegerValidator.lessThanOrEqualTo(4096)));
                fields.put("TCDirectProjectID", LongValidator.isPositive());
                break;

            default:
                break;
        }

        searchBundle.setSearchableFields(fields);
    }
}
