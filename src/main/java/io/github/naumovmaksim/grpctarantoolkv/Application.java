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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class Application {
    private final static Logger log = LoggerFactory.getLogger(Application.class);
    //Константы нужно выность например в .env, но тут они захрдкожены намеренно в целях
    // более простого и быстрого запуска
    private static final int GRPC_PORT =
            Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "9090"));

    private static final String TARANTOOL_HOST =
            System.getenv().getOrDefault("TARANTOOL_HOST", "localhost");

    private static final int TARANTOOL_PORT =
            Integer.parseInt(System.getenv().getOrDefault("TARANTOOL_PORT", "3301"));

    private static final String TARANTOOL_USER =
            System.getenv().getOrDefault("TARANTOOL_USER", "app");

    private static final String TARANTOOL_PASSWORD =
            System.getenv().getOrDefault("TARANTOOL_PASSWORD", "secret");

    private Server server;
    private TarantoolKvRepository tarantoolKvRepository;

    public static void main(String[] args) throws IOException, InterruptedException {
        Application application = new Application();
        application.start();
        application.blockUntilShutdown();
    }

    private void start() throws IOException {
        log.info("Starting application");

        tarantoolKvRepository = createTarantoolRepository();

        KvRepository repository = tarantoolKvRepository;

        server = ServerBuilder.forPort(GRPC_PORT)
                .addService(new KvServiceImpl(repository))
                .build()
                .start();

        log.info("Connected to Tarantool on {}:{} ", TARANTOOL_HOST, TARANTOOL_PORT);
        log.info("gRPC server started on port {} ", GRPC_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down server");
            Application.this.stop();
            log.info("Server stopped");
        }));
    }

    private TarantoolKvRepository createTarantoolRepository() {
        log.info("Creating Tarantool connection group for {}:{}", TARANTOOL_HOST, TARANTOOL_PORT);

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
            log.error("Failed to build Tarantool client");
            throw new RuntimeException("Failed to build Tarantool client ",e);
        }

        log.info("Tarantool client created");
        return new TarantoolKvRepository(client);
    }

    private void stop() {
        log.info("Stopping application");

        if (server != null) {
            server.shutdown();
            log.info("gRPC server shutting down");
        }

        if (tarantoolKvRepository != null) {
            try {
                tarantoolKvRepository.close();
                log.info("Tarantool client closed");
            } catch (Exception e) {
                log.error("Failed to close Tarantool client");
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