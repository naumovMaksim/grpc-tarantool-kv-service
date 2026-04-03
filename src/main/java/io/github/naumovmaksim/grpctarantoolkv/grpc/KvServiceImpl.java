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
import io.github.naumovmaksim.grpctarantoolkv.validation.RequestValidator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
        try {
            RequestValidator.validateKey(request.getKey(), "key");

            byte[] val = null;
            if (request.hasValue()) {
                val = request.getValue().toByteArray();
            }

            repository.put(request.getKey(), val);

            PutResponse resp = PutResponse.newBuilder().build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError(e, responseObserver);
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            RequestValidator.validateKey(request.getKey(), "key");

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
        } catch (Exception e) {
            handleError(e, responseObserver);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            RequestValidator.validateKey(request.getKey(), "key");

            boolean deleted = repository.delete(request.getKey());

            DeleteResponse response = DeleteResponse.newBuilder()
                    .setDeleted(deleted)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError(e, responseObserver);
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        try {
            RequestValidator.validateRange(request.getKeySince(), request.getKeyTo());

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
        } catch (Exception e) {
            handleError(e, responseObserver);
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            long count = repository.count();

            CountResponse response = CountResponse.newBuilder()
                    .setCount(count)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError(e, responseObserver);
        }
    }

    private void handleError(Exception exception, StreamObserver<?> responseObserver) {
        if (exception instanceof StatusRuntimeException) {
            responseObserver.onError(exception);
            return;
        }

        if (exception instanceof IllegalArgumentException) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(exception.getMessage())
                            .asRuntimeException()
            );
            return;
        }

        if (exception instanceof RuntimeException) {
            responseObserver.onError(
                    Status.UNAVAILABLE
                            .withDescription("Storage is unavailable")
                            .withCause(exception)
                            .asRuntimeException()
            );
            return;
        }

        responseObserver.onError(
                Status.INTERNAL
                        .withDescription("Unexpected server error")
                        .withCause(exception)
                        .asRuntimeException()
        );
    }
}