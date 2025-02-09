package com.easydb.storage.constraint;

public class ConstraintViolationException extends IllegalArgumentException {
    
    public ConstraintViolationException(String errorMessage) {
        super(errorMessage);
    }
}
