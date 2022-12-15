package com.topcoder.or.util;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.google.protobuf.Timestamp;
import com.topcoder.onlinereview.component.datavalidator.IntegerValidator;
import com.topcoder.onlinereview.component.datavalidator.ObjectValidator;
import com.topcoder.onlinereview.component.datavalidator.StringValidator;
import com.topcoder.onlinereview.component.search.SearchBundle;

public final class Helper {

    /**
     * Identifier of deliverable search bundle.
     */
    public static final int DELIVERABLE_SEARCH_BUNDLE = 1;

    /**
     * Identifier of deliverable with submission search bundle.
     */
    public static final int DELIVERABLE_WITH_SUBMISSIONS_SEARCH_BUNDLE = 2;

    /**
     * Identifier of upload search bundle.
     */
    public static final int UPLOAD_SEARCH_BUNDLE = 3;

    /**
     * Identifier of submission search bundle.
     */
    public static final int SUBMISSION_SEARCH_BUNDLE = 4;

    /**
     * Check if the given object is null.
     *
     * @param verifier method to be used to verify object
     * @param name     the name to identify the object.
     * @throws IllegalArgumentException if the given object is null
     */
    public static void assertObjectNotNull(Supplier<Boolean> verifier, String name) {
        if (!verifier.get()) {
            throw new IllegalArgumentException("%s is required".formatted(name));
        }
    }

    /**
     * Check if the given object is null and positive.
     *
     * @param verifier method to be used to verify object
     * @param get      method to be used to get object
     * @param name     the name to identify the object.
     * @throws IllegalArgumentException if the given object is null
     */
    public static void assertObjectNotNullAndPositive(Supplier<Boolean> verifier, Supplier<Number> get, String name) {
        if (!verifier.get() || get.get().intValue() < 1) {
            throw new IllegalArgumentException("%s is required".formatted(name));
        }
    }

    /**
     * Check if the given object is not empty
     *
     * @param counter method to be used to count object
     * @param name    the name to identify the object.
     * @throws IllegalArgumentException if the given object is null
     */
    public static void assertObjectNotEmpty(Supplier<Integer> counter, String name) {
        if (counter.get() == 0) {
            throw new IllegalArgumentException("%s is required".formatted(name));
        }
    }

    public static <T> T extract(Supplier<Boolean> verifier, Supplier<T> extrator) {
        return verifier.get() ? extrator.get() : null;
    }

    public static Date extractDate(Supplier<Boolean> verifier, Supplier<Timestamp> extractor) {
        return verifier.get() ? new Date(extractor.get().getSeconds() * 1000) : null;
    }

    public static Date convertDate(Timestamp date) {
        return new Date(date.getSeconds() * 1000);
    }

    public static String getInClause(Integer count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

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
                fields.put("submission_id", greaterThanZeroValidator);
                // Falls through.

            case DELIVERABLE_SEARCH_BUNDLE:
                fields.put("deliverable_id", greaterThanZeroValidator);
                fields.put("phase_id", greaterThanZeroValidator);
                fields.put("name", StringValidator.hasLength(greaterThanZeroValidator));
                fields.put("required", IntegerValidator.inRange(Integer.MIN_VALUE, Integer.MAX_VALUE));
                break;

            case UPLOAD_SEARCH_BUNDLE:
                fields.put("upload_id", greaterThanZeroValidator);
                fields.put("upload_type_id", greaterThanZeroValidator);
                fields.put("upload_status_id", greaterThanZeroValidator);
                fields.put("project_phase_id", greaterThanZeroValidator);
                break;

            case SUBMISSION_SEARCH_BUNDLE:
                fields.put("upload_id", greaterThanZeroValidator);
                fields.put("submission_id", greaterThanZeroValidator);
                fields.put("submission_status_id", greaterThanZeroValidator);
                fields.put("submission_type_id", greaterThanZeroValidator);
                fields.put("project_phase_id", greaterThanZeroValidator);
                break;

            default:
                break;
        }

        // Set common searchable fields for those search bundle.
        fields.put("project_id", greaterThanZeroValidator);
        fields.put("resource_id", greaterThanZeroValidator);

        searchBundle.setSearchableFields(fields);
    }
}
