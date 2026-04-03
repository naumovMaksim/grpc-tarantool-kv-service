package io.github.naumovmaksim.grpctarantoolkv.validation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RequestValidatorTest {

    @Test
    void validateKey_shouldThrow_whenKeyIsBlank() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RequestValidator.validateKey("   ", "key")
        );

        assertEquals(
                "key must not be blank",
                exception.getMessage()
        );
    }

    @Test
    void validateRange_shouldThrow_whenKeySinceGreaterThanKeyTo() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RequestValidator.validateRange("z", "a")
        );

        assertEquals(
                "key_since must be less than or equal to key_to",
                exception.getMessage()
        );
    }

    @Test
    void validateRange_shouldNotThrow_whenRangeIsCorrect() {
        assertDoesNotThrow(() -> RequestValidator.validateRange("a", "z"));
    }
}
