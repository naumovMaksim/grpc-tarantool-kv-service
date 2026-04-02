package io.github.naumovmaksim.grpctarantoolkv.model;

import java.util.Arrays;

public class KvRecord {

    private final String key;
    private final byte[] value;

    public KvRecord(String key, byte[] value) {
        this.key = key;
        this.value = copy(value);
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return copy(value);
    }

    //Массив bute[] - изменяемый, поэтому делаем копию, чтобы код снаружи не мог его изменить
    private byte[] copy(byte[] source) {
        if (source == null) {
            return null;
        }
        return Arrays.copyOf(source, source.length);
    }
}