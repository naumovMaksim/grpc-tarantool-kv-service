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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(KvServiceImpl.class);

    private final KvRepository repository;

    public KvServiceImpl(KvRepository repository) {
        this.repository = repository;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        log.info("PUT request received: key={}, hasValue={}", request.getKey(), request.hasValue());

        try {
            RequestValidator.validateKey(request.getKey(), "key");

            byte[] val = null;
            if (request.hasValue()) {
                val = request.getValue().toByteArray();
            }

            repository.put(request.getKey(), val);

            log.info("PUT completed: key={}", request.getKey());

            PutResponse resp = PutResponse.newBuilder().build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError("PUT", request.getKey(), e, responseObserver);
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        log.info("GET request received: key={}", request.getKey());

        try {
            RequestValidator.validateKey(request.getKey(), "key");

            Optional<KvRecord> optionalRecord = repository.get(request.getKey());

            GetResponse.Builder builder = GetResponse.newBuilder()
                    .setKey(request.getKey());

            if (optionalRecord.isEmpty()) {
                builder.setFound(false);
                log.info("GET completed: key={}, found=false", request.getKey());
            } else {
                KvRecord record = optionalRecord.get();
                builder.setFound(true);

                if (record.getValue() != null) {
                    builder.setValue(ByteString.copyFrom(record.getValue()));
                }

                log.info("GET completed: key='{}', found=true, hasValue={}", request.getKey(),
                        record.getValue() != null);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError("Get", request.getKey(), e, responseObserver);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        log.info("DELETE request received: key={}", request.getKey());

        try {
            RequestValidator.validateKey(request.getKey(), "key");

            boolean deleted = repository.delete(request.getKey());

            log.info("DELETE completed: key='{}', deleted={}", request.getKey(), deleted);

            DeleteResponse response = DeleteResponse.newBuilder()
                    .setDeleted(deleted)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError("DELETE", request.getKey(), e, responseObserver);
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        log.info("RANGE request received: keySince='{}', keyTo='{}'", request.getKeySince(), request.getKeyTo());

        try {
            RequestValidator.validateRange(request.getKeySince(), request.getKeyTo());

            List<KvRecord> records = repository.range(request.getKeySince(), request.getKeyTo());

            int sent = 0;

            for (KvRecord record : records) {
                RangeResponse.Builder builder = RangeResponse.newBuilder()
                        .setKey(record.getKey());

                if (record.getValue() != null) {
                    builder.setValue(ByteString.copyFrom(record.getValue()));
                }

                responseObserver.onNext(builder.build());
                sent++;
            }

            log.info("RANGE completed: keySince='{}', keyTo='{}', sent={}",
                    request.getKeySince(),
                    request.getKeyTo(),
                    sent);

            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError("RANGE", request.getKeySince() + ".." + request.getKeyTo(), e, responseObserver);
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        log.info("COUNT request received");

        try {
            long count = repository.count();

            log.info("COUNT completed: count={}", count);

            CountResponse response = CountResponse.newBuilder()
                    .setCount(count)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleError("COUNT", "-", e, responseObserver);
        }
    }

    private void handleError(String operation,
                             String keyInfo,
                             Exception exception,
                             StreamObserver<?> responseObserver) {
        if (exception instanceof StatusRuntimeException) {
            log.error("{} failed for keyInfo='{}': gRPC status exception", operation, keyInfo, exception);
            responseObserver.onError(exception);
            return;
        }

        if (exception instanceof IllegalArgumentException) {
            log.warn("{} validation failed for keyInfo='{}': {}", operation, keyInfo, exception.getMessage());
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(exception.getMessage())
                            .asRuntimeException()
            );
            return;
        }

        if (exception instanceof RuntimeException) {
            log.error("{} failed for keyInfo='{}': storage unavailable", operation, keyInfo, exception);
            responseObserver.onError(
                    Status.UNAVAILABLE
                            .withDescription("Storage is unavailable")
                            .withCause(exception)
                            .asRuntimeException()
            );
            return;
        }

        log.error("{} failed for keyInfo='{}': unexpected server error", operation, keyInfo, exception);
        responseObserver.onError(
                Status.INTERNAL
                        .withDescription("Unexpected server error")
                        .withCause(exception)
                        .asRuntimeException()
        );
    }
}