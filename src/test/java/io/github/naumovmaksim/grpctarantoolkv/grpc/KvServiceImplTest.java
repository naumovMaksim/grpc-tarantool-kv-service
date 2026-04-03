package io.github.naumovmaksim.grpctarantoolkv.grpc;

import com.google.protobuf.ByteString;
import io.github.naumovmaksim.grpctarantoolkv.proto.CountRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.CountResponse;
import io.github.naumovmaksim.grpctarantoolkv.proto.DeleteRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.DeleteResponse;
import io.github.naumovmaksim.grpctarantoolkv.proto.GetRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.GetResponse;
import io.github.naumovmaksim.grpctarantoolkv.proto.KvServiceGrpc;
import io.github.naumovmaksim.grpctarantoolkv.proto.PutRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.RangeRequest;
import io.github.naumovmaksim.grpctarantoolkv.proto.RangeResponse;
import io.github.naumovmaksim.grpctarantoolkv.repository.InMemoryKvRepository;
import io.github.naumovmaksim.grpctarantoolkv.repository.KvRepository;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KvServiceImplTest {

    private Server server;
    private ManagedChannel channel;
    private KvServiceGrpc.KvServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        KvRepository repository = new InMemoryKvRepository();
        KvServiceImpl service = new KvServiceImpl(repository);

        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(service)
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        stub = KvServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }

        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void putAndGet_shouldReturnStoredValue() {
        PutRequest putRequest = PutRequest.newBuilder()
                .setKey("alpha")
                .setValue(ByteString.copyFrom("sad", StandardCharsets.UTF_8))
                .build();

        stub.put(putRequest);

        GetResponse response = stub.get(
                GetRequest.newBuilder()
                        .setKey("alpha")
                        .build()
        );

        assertTrue(response.getFound());
        assertEquals("alpha", response.getKey());
        assertTrue(response.hasValue());
        assertEquals("sad", response.getValue().toString(StandardCharsets.UTF_8));
    }

    @Test
    void putAndGet_shouldSupportNullValue() {
        PutRequest putRequest = PutRequest.newBuilder()
                .setKey("beta")
                .build();

        stub.put(putRequest);

        GetResponse response = stub.get(
                GetRequest.newBuilder()
                        .setKey("beta")
                        .build()
        );

        assertTrue(response.getFound());
        assertEquals("beta", response.getKey());
        assertFalse(response.hasValue());
    }

    @Test
    void deleteCountAndRange_shouldWorkCorrectly() {
        stub.put(PutRequest.newBuilder()
                .setKey("a")
                .setValue(ByteString.copyFrom("one", StandardCharsets.UTF_8))
                .build());

        stub.put(PutRequest.newBuilder()
                .setKey("b")
                .build());

        stub.put(PutRequest.newBuilder()
                .setKey("c")
                .setValue(ByteString.copyFrom("three", StandardCharsets.UTF_8))
                .build());

        CountResponse countBeforeDelete = stub.count(CountRequest.newBuilder().build());
        assertEquals(3, countBeforeDelete.getCount());

        Iterator<RangeResponse> iterator = stub.range(
                RangeRequest.newBuilder()
                        .setKeySince("a")
                        .setKeyTo("c")
                        .build()
        );

        List<String> keys = new ArrayList<>();
        List<Boolean> hasValue = new ArrayList<>();

        while (iterator.hasNext()) {
            RangeResponse response = iterator.next();
            keys.add(response.getKey());
            hasValue.add(response.hasValue());
        }

        assertEquals(List.of("a", "b", "c"), keys);
        assertEquals(List.of(true, false, true), hasValue);

        DeleteResponse deleteResponse = stub.delete(
                DeleteRequest.newBuilder()
                        .setKey("a")
                        .build()
        );

        assertTrue(deleteResponse.getDeleted());

        CountResponse countAfterDelete = stub.count(CountRequest.newBuilder().build());
        assertEquals(2, countAfterDelete.getCount());
    }

    @Test
    void get_shouldReturnInvalidArgument_whenKeyIsBlank() {
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.get(
                        GetRequest.newBuilder()
                                .setKey("   ")
                                .build()
                )
        );

        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals("key must not be blank", exception.getStatus().getDescription());
    }
}