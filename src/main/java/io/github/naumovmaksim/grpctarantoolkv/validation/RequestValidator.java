package io.github.naumovmaksim.grpctarantoolkv.validation;

public final class RequestValidator {

    private RequestValidator() {
    }

    public static void validateKey(String key, String fieldName) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    public static void validateRange(String keySince, String keyTo) {
        validateKey(keySince, "key_since");
        validateKey(keyTo, "key_to");

        if (keySince.compareTo(keyTo) > 0) {
            throw new IllegalArgumentException("key_since must be less than or equal to key_to");
        }
    }
}
