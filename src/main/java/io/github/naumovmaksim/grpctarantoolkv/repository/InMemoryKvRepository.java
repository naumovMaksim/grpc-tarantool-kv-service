package io.github.naumovmaksim.grpctarantoolkv.repository;

import io.github.naumovmaksim.grpctarantoolkv.model.KvRecord;

import java.util.*;

public class InMemoryKvRepository implements KvRepository {


    private final NavigableMap<String, byte[]> storage = Collections.synchronizedNavigableMap(new TreeMap<>());

    @Override
    public void put(String key, byte[] value) {
        storage.put(key, copy(value));
    }

    @Override
    public Optional<KvRecord> get(String key) {
        synchronized (storage) {
            if (!storage.containsKey(key)) {
                return Optional.empty();
            }

            byte[] value = storage.get(key);
            return Optional.of(new KvRecord(key, value));
        }
    }

    @Override
    public boolean delete(String key) {
        synchronized (storage) {
            if (!storage.containsKey(key)) {
                return false;
            }

            storage.remove(key);
            return true;
        }
    }

    @Override
    public List<KvRecord> range(String keySince, String keyTo) {
        if (keySince.compareTo(keyTo) > 0) {
            return Collections.emptyList();
        }

        synchronized (storage) {
            NavigableMap<String, byte[]> subMap =
                    storage.subMap(keySince, true, keyTo, true);

            List<KvRecord> result = new ArrayList<>();

            for (var entry : subMap.entrySet()) {
                result.add(new KvRecord(entry.getKey(), entry.getValue()));
            }

            return result;
        }
    }

    @Override
    public long count() {
        synchronized (storage) {
            return storage.size();
        }
    }

    //Массив bute[] - изменяемый, поэтому делаем копию, чтобы код снаружи не мог его изменить
    private byte[] copy(byte[] source) {
        if (source == null) {
            return null;
        }

        return Arrays.copyOf(source, source.length);
    }
}