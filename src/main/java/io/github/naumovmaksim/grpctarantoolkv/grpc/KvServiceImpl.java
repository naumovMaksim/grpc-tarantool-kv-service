package io.github.naumovmaksim.grpctarantoolkv.grpc;

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
import io.grpc.stub.StreamObserver;

public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        PutResponse response = PutResponse.newBuilder().build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        GetResponse response = GetResponse.newBuilder()
                .setFound(false)
                .setKey(request.getKey())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        DeleteResponse response = DeleteResponse.newBuilder()
                .setDeleted(false)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        responseObserver.onCompleted();
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        CountResponse response = CountResponse.newBuilder()
                .setCount(0)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}