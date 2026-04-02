package io.github.naumovmaksim.grpctarantoolkv;

import io.github.naumovmaksim.grpctarantoolkv.grpc.KvServiceImpl;
import io.github.naumovmaksim.grpctarantoolkv.repository.InMemoryKvRepository;
import io.github.naumovmaksim.grpctarantoolkv.repository.KvRepository;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Application {

    private static final int PORT = 9090;

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        Application application = new Application();
        application.start();
        application.blockUntilShutdown();
    }

    private void start() throws IOException {
        KvRepository repository = new InMemoryKvRepository();

        server = ServerBuilder.forPort(PORT)
                .addService(new KvServiceImpl(repository))
                .build()
                .start();

        System.out.println("gRPC server started on port " + PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gRPC server...");
            Application.this.stop();
            System.out.println("Server stopped.");
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}