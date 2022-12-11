package com.topcoder.or.repository;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class Validation {

    private boolean valid = true;
    private String message;

    public void setMessage(String message) {
        this.message = message;
        valid = false;
    }

    public void setRequiredFieldMessage(String fieldName) {
        message = "%s is required".formatted(fieldName);
        valid = false;
    }
}
