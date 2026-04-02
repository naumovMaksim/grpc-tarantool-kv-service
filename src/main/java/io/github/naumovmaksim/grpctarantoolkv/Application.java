package io.github.naumovmaksim.grpctarantoolkv;

import io.github.naumovmaksim.grpctarantoolkv.grpc.KvServiceImpl;
import io.github.naumovmaksim.grpctarantoolkv.repository.KvRepository;
import io.github.naumovmaksim.grpctarantoolkv.repository.tarantool.TarantoolKvRepository;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolBoxClientBuilder;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;

import java.io.IOException;
import java.util.Collections;

public class Application {
    //Константы нужно выность например в .env, но тут они захрдкожены намеренно в целях
    // более простого и быстрого запуска
    private static final int PORT = 9090;

    private static final String TARANTOOL_HOST = "localhost";
    private static final int TARANTOOL_PORT = 3301;
    private static final String TARANTOOL_USER = "app";
    private static final String TARANTOOL_PASSWORD = "secret";

    private Server server;
    private TarantoolKvRepository tarantoolKvRepository;

    public static void main(String[] args) throws IOException, InterruptedException {
        Application application = new Application();
        application.start();
        application.blockUntilShutdown();
    }

    private void start() throws IOException {
        tarantoolKvRepository = createTarantoolRepository();

        KvRepository repository = tarantoolKvRepository;

        server = ServerBuilder.forPort(PORT)
                .addService(new KvServiceImpl(repository))
                .build()
                .start();

        System.out.println("Connected to Tarantool on " + TARANTOOL_HOST + ":" + TARANTOOL_PORT);
        System.out.println("gRPC server started on port " + PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gRPC server...");
            Application.this.stop();
            System.out.println("Server stopped.");
        }));
    }

    private TarantoolKvRepository createTarantoolRepository() {
        InstanceConnectionGroup connectionGroup = InstanceConnectionGroup.builder()
                .withHost(TARANTOOL_HOST)
                .withPort(TARANTOOL_PORT)
                .withUser(TARANTOOL_USER)
                .withPassword(TARANTOOL_PASSWORD)
                .build();

        TarantoolBoxClientBuilder clientBuilder =
                TarantoolFactory.box().withGroups(Collections.singletonList(connectionGroup));

        TarantoolBoxClient client = null;
        try {
            client = clientBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build Tarantool client ",e);
        }

        return new TarantoolKvRepository(client);
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }

        if (tarantoolKvRepository != null) {
            try {
                tarantoolKvRepository.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close Tarantool client ", e);
            }
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}