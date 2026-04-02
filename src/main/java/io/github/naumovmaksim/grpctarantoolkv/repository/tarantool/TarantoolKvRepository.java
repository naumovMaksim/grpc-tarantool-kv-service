package io.github.naumovmaksim.grpctarantoolkv.repository.tarantool;

import io.github.naumovmaksim.grpctarantoolkv.model.KvRecord;
import io.github.naumovmaksim.grpctarantoolkv.repository.KvRepository;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.TarantoolResponse;
import io.tarantool.mapping.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TarantoolKvRepository implements KvRepository, AutoCloseable {

    private static final String SPACE_NAME = "KV";
    private static final int RANGE_BATCH_SIZE = 1000;

    private final TarantoolBoxClient client;
    private final TarantoolBoxSpace space;

    public TarantoolKvRepository(TarantoolBoxClient client) {
        this.client = client;
        this.space = client.space(SPACE_NAME);
    }

    @Override
    public void put(String key, byte[] value) {
        try {
            TarantoolKvTuple tuple = new TarantoolKvTuple(key, copy(value));
            space.replace(tuple, TarantoolKvTuple.class).join();
        } catch (Exception e) {
            throw new RuntimeException("Failed to put record into Tarantool", e);
        }
    }

    @Override
    public Optional<KvRecord> get(String key) {
        try {
            SelectResponse<List<Tuple<TarantoolKvTuple>>> response =
                    space.select(Collections.singletonList(key), TarantoolKvTuple.class).join();

            List<Tuple<TarantoolKvTuple>> tuples = response.get();

            if (tuples.isEmpty()) {
                return Optional.empty();
            }

            TarantoolKvTuple tuple = tuples.getFirst().get();
            return Optional.of(toRecord(tuple));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get record from Tarantool", e);
        }
    }

    @Override
    public boolean delete(String key) {
        Optional<KvRecord> existing = get(key);

        if (existing.isEmpty()) {
            return false;
        }

        try {
            space.delete(Collections.singletonList(key), TarantoolKvTuple.class).join();
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete record from Tarantool", e);
        }
    }

    @Override
    public List<KvRecord> range(String keySince, String keyTo) {
        List<KvRecord> result = new ArrayList<>();

        if (keySince.compareTo(keyTo) > 0) {
            throw new RuntimeException("Failed to read range from Tarantool because of wrong keys");
        }

        String currentKey = keySince;
        boolean firstBatch = true;

        while (true) {
            SelectOptions options = SelectOptions.builder()
                    .withLimit(RANGE_BATCH_SIZE)
                    .withIterator(firstBatch ? BoxIterator.GE : BoxIterator.GT)
                    .build();

            try {
                SelectResponse<List<Tuple<TarantoolKvTuple>>> response =
                        space.select(Collections.singletonList(currentKey), options, TarantoolKvTuple.class).join();

                List<Tuple<TarantoolKvTuple>> tuples = response.get();

                if (tuples.isEmpty()) {
                    break;
                }

                boolean reachedUpperBound = false;

                for (Tuple<TarantoolKvTuple> tupleWrapper : tuples) {
                    TarantoolKvTuple tuple = tupleWrapper.get();

                    if (tuple.key.compareTo(keyTo) > 0) {
                        reachedUpperBound = true;
                        break;
                    }

                    result.add(toRecord(tuple));
                    currentKey = tuple.key;
                }

                if (reachedUpperBound || tuples.size() < RANGE_BATCH_SIZE) {
                    break;
                }

                firstBatch = false;
            } catch (Exception e) {
                throw new RuntimeException("Failed to read range from Tarantool", e);
            }
        }

        return result;
    }

    @Override
    public long count() {
        try {
            TarantoolResponse<List<Long>> response =
                    client.eval("return box.space.KV:count()", Long.class).join();

            List<Long> values = response.get();

            if (values.isEmpty()) {
                return 0;
            }

            return values.getFirst();
        } catch (Exception e) {
            throw new RuntimeException("Failed to count records in Tarantool", e);
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close Tarantool client",e);
        }
    }

    private KvRecord toRecord(TarantoolKvTuple tuple) {
        return new KvRecord(tuple.key, tuple.value);
    }

    //Массив byte[] - изменяемый, поэтому делаем копию, чтобы код снаружи не мог его изменить
    private byte[] copy(byte[] source) {
        if (source == null) {
            return null;
        }
        return Arrays.copyOf(source, source.length);
    }
}