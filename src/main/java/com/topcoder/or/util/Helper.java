package com.topcoder.or.util;

import java.util.Date;
import java.util.function.Supplier;

import com.google.protobuf.Timestamp;

public final class Helper {

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

    public static <T> T extract(Supplier<Boolean> verifier, Supplier<T> extrator) {
        return verifier.get() ? extrator.get() : null;
    }

    public static Date extractDate(Supplier<Boolean> verifier, Supplier<Timestamp> extractor) {
        return verifier.get() ? new Date(extractor.get().getSeconds() * 1000) : null;
    }
}
