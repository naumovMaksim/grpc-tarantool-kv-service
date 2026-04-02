package io.github.naumovmaksim.grpctarantoolkv.repository;

import io.github.naumovmaksim.grpctarantoolkv.model.KvRecord;

import java.util.List;
import java.util.Optional;

public interface KvRepository {

    void put(String key, byte[] value);

    Optional<KvRecord> get(String key);

    boolean delete(String key);

    List<KvRecord> range(String keySince, String keyTo);

    long count();
}