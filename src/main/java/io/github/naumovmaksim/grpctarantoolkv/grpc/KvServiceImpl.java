package io.github.naumovmaksim.grpctarantoolkv.grpc;

import com.google.protobuf.ByteString;
import io.github.naumovmaksim.grpctarantoolkv.model.KvRecord;
import io.github.naumovmaksim.grpctarantoolkv.proto.CountRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.CountResponse;
import io.github.naumovmaksim.grpctarantoolkv.proto.DeleteRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.DeleteResponse;
import io.github.naumovmaksim.grpctarantoolkv.proto.GetRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.GetResponse;
import io.github.naumovmaksim.grpctarantoolkv.proto.KvServiceGrpc;
import io.github.naumovmaksim.grpctarantoolkv.proto.PutRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.PutResponse;
import io.github.naumovmaksim.grpctarantoolkv.proto.RangeRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.RangeResponse;
import io.github.naumovmaksim.grpctarantoolkv.repository.KvRepository;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Optional;

public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final KvRepository repository;

    public KvServiceImpl(KvRepository repository) {
        this.repository = repository;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        byte[] value = null;

        if (request.hasValue()) {
            value = request.getValue().toByteArray();
        }

        repository.put(request.getKey(), value);

        PutResponse response = PutResponse.newBuilder().build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        Optional<KvRecord> optionalRecord = repository.get(request.getKey());

        GetResponse.Builder builder = GetResponse.newBuilder()
                .setKey(request.getKey());

        if (optionalRecord.isEmpty()) {
            builder.setFound(false);
        } else {
            KvRecord record = optionalRecord.get();
            builder.setFound(true);

            if (record.getValue() != null) {
                builder.setValue(ByteString.copyFrom(record.getValue()));
            }
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        boolean deleted = repository.delete(request.getKey());

        DeleteResponse response = DeleteResponse.newBuilder()
                .setDeleted(deleted)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        List<KvRecord> records = repository.range(request.getKeySince(), request.getKeyTo());

        for (KvRecord record : records) {
            RangeResponse.Builder builder = RangeResponse.newBuilder()
                    .setKey(record.getKey());

            if (record.getValue() != null) {
                builder.setValue(ByteString.copyFrom(record.getValue()));
            }

            responseObserver.onNext(builder.build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        long count = repository.count();

        CountResponse response = CountResponse.newBuilder()
                .setCount(count)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}