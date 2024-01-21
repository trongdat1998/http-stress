package io.bhex.broker.stress.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.bhex.base.metrics.PrometheusMetricsCollector;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/internal/quote/ws")
@Slf4j
public class QuoteWsStressController {

    private ThreadPoolExecutor executor;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(20);
    private Future<?> future;
    private final static Map<Integer, Runnable> RUNNABLE_MAP = new ConcurrentHashMap<>();

    public static final Histogram requestLatency = Histogram.build()
            .name("quote_ws_latency_milliseconds")
            .help("Websocket latency in milliseconds.")
            .labelNames("type")
            .buckets(PrometheusMetricsCollector.RPC_TIME_BUCKETS)
            .register();

    // type
    // 0     0      0      0
    // kline trade depth ticker
    @GetMapping("/stress")
    public String stress(
            @RequestParam("host") String host,
            @RequestParam("port") int port,
            @RequestParam("symbol") String symbol,
            @RequestParam("c") int c) {

        if (executor != null) {
            return "stop firstly";
        }
        executor = new ThreadPoolExecutor(c, c,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(c),
                new DefaultThreadFactory("quote-ws-thread-factory"), new ThreadPoolExecutor.CallerRunsPolicy());
        executor.prestartAllCoreThreads();

        if (future != null) {
            future.cancel(true);
            future = null;
        }


        try {
            log.info("start ws");
            for (int i = 0; i < c; ++i) {
                executor.submit(new QuoteWsTask(host, port, symbol, executor));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return "OK";
    }

    @GetMapping("/stop")
    public String stop() {
        executor.shutdownNow();
        executor = null;
        return "OK";
    }

    public static class QuoteWsTask implements Runnable {
        private String url;
        private String symbol;
        private ExecutorService executorService;

        public QuoteWsTask(String host, int port, String symbol, ExecutorService executorService) {
            this.url = buildUrl(host, port);
            this.symbol = symbol;
            this.executorService = executorService;
        }

        @Override
        public void run() {
            try {
                connectBhexWs();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        private void connectBhexWs() {
            log.info("Connecting ws");
            ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
            HttpHeaders headers = new HttpHeaders();
            headers.put("Host", Collections.singletonList("ws.bhex.io"));
            client.execute(URI.create(this.url), headers,
                    session -> {
                        log.info("ws connected");
                        UnicastProcessor<WebSocketMessage> hotSource = UnicastProcessor.create();
                        Flux<WebSocketMessage> hotFlux = hotSource.publish().autoConnect();
                        Mono<Void> output = session.send(hotFlux);
                        Thread hb = new Thread(() -> {
                            while (true) {
                                hotSource.onNext(session
                                        .textMessage("{\"ping\": " + System.currentTimeMillis() + "}"));
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    break;
                                }
                            }

                        });
                        hotSource.onNext(session.textMessage("{\"topic\": \"trade\", \"event\": \"sub\", \"params\": {\"symbol\": \"" + symbol + "\"}}"));
                        hotSource.onNext(session.textMessage("{\"topic\": \"realtimes\", \"event\": \"sub\", \"params\": {\"symbol\": \"" + symbol + "\"}}"));
                        hotSource.onNext(session.textMessage("{\"topic\": \"depth\", \"event\": \"sub\", \"params\": {\"symbol\": \"" + symbol + "\"}}"));
                        Mono<Void> input = session
                                .receive()
                                .doOnNext(message -> {
                                    if (WebSocketMessage.Type.TEXT.equals(message.getType())) {
                                        onMessage(message.getPayloadAsText(StandardCharsets.UTF_8));
                                    }
                                })
                                .then();

                        hb.start();
                        return Mono.zip(input, output).then()
                                .doOnError(c -> log.error(c.getMessage(), c))
                                .doOnTerminate(hb::interrupt);
                    })
                    .doOnSuccess(aVoid -> log.info("Success connect to Bhex WebSocket."))
                    .doOnError(e -> log.error(e.getMessage(), e))
                    .doOnTerminate(() -> executorService.submit(this::connectBhexWs))
                    .block();
        }

        private void onMessage(String msg) {
            try {
                JSONObject jo = JSON.parseObject(msg);
                if (jo.containsKey("pong")) {
                    return;
                }
                if (jo.containsKey("code")) {
                    int code = jo.getInteger("code");
                    if (code == 0) {
                        return;
                    }
                }
                String topic = jo.getString("topic");
                JSONObject data = jo.getJSONObject("data");
                if(data == null) {
                    log.error("jo: {}", jo);
                    return;
                }
                if(data.getLong("t") == null) {
                    log.error("data: {}", data);
                    return;
                }
                long time = data.getLong("t");
                metricsLatency(topic, time);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        // type
        // 0     0      0      0
        // kline trade depth ticker
        private String buildUrl(String host, int port) {
            //return "ws://ws.hbtc.co/openapi/quote/ws/v2";
            return "ws://" + host + ":" + port + "/openapi/quote/ws/v2";
        }
    }

    private static void metricsLatency(String type, long start) {
        requestLatency
                .labels(type)
                .observe(System.currentTimeMillis() - start);
    }
}
