import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.io.FileUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Example {
    static ThreadPoolExecutor tpe;

    public static void main(String[] args) {
        tpe = new ThreadPoolExecutor(20, 40, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(50));
        DisposableServer server = HttpServer.create()
                .host("0.0.0.0")
                .port(8080)
                .route(routes ->
                        routes
                                .get("/", Example::hello)
                                .post("/", Example::saveData)
                )
                .bindNow();
        server.onDispose().block();
    }

    private static Publisher<Void> hello(HttpServerRequest req, HttpServerResponse res) {
        return res.status(HttpResponseStatus.OK).sendString(Mono.just("OK"));
    }

    private static Publisher<Void> saveData(HttpServerRequest request, HttpServerResponse response) {
        return request
                .receive()
                .aggregate()
                .asByteArray()
                .publishOn(Schedulers.fromExecutorService(tpe))
                .handle((data, sink) -> {
                    try {
                        String fileName = String.format("./example-%s.txt", UUID.randomUUID());
                        FileUtils.writeByteArrayToFile(new File(fileName), data);
                        sink.next(data.length);
                    }
                    catch (IOException e) {
                        sink.error(e);
                    }
                })
                .doOnSuccess(dataLegnth -> {
                    response.status(200).sendString(Mono.just(String.format("OK Saved %d bytes.", dataLegnth))).then().subscribe();
                })
                .doOnError(ex -> {
                    response.status(500).sendString(Mono.just(String.format("ERROR %s" ,ex.getMessage()))).then().subscribe();
                })
                .then();

    }
}
