package io.github.naumovmaksim.grpctarantoolkv.repository.tarantool;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.ARRAY)
public class TarantoolKvTuple {

    public String key;
    public byte[] value;

    public TarantoolKvTuple() {
    }

    public TarantoolKvTuple(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}