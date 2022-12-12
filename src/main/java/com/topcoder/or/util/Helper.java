package com.topcoder.or.util;

import java.util.function.Supplier;

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
}
