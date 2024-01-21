package io.bhex.broker.stress.controller;

import io.bhex.base.metrics.PrometheusMetricsCollector;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/internal/quote")
@Slf4j
public class QuoteStressController {

    private ThreadPoolExecutor executor;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(20);
    private Future<?> future;
    private final static Map<Integer, Runnable> RUNNABLE_MAP = new ConcurrentHashMap<>();

    public static final Histogram requestLatency = Histogram.build()
        .name("quote_requests_latency_milliseconds")
        .help("Request latency in milliseconds.")
        .labelNames("type", "code")
        .buckets(PrometheusMetricsCollector.RPC_TIME_BUCKETS)
        .register();

    // type
    // 0     0      0      0
    // kline trade depth ticker
    @GetMapping("/stress")
    public String stress(
        @RequestParam("host") String host,
        @RequestParam("port") int port,
        @RequestParam("type") int type,
        @RequestParam("symbol") String symbol,
        @RequestParam("qsize") int qsize,
        @RequestParam("c") int c,
        @RequestParam("qps") int qps) {

        if (executor != null) {
            return "stop firstly";
        }
        executor = new ThreadPoolExecutor(c, c,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(qsize),
            new DefaultThreadFactory("quote-thread-factory"), new ThreadPoolExecutor.DiscardOldestPolicy());
        executor.prestartAllCoreThreads();

        RUNNABLE_MAP.put(1, new QuoteTask(host, port, 1, symbol));
        RUNNABLE_MAP.put(2, new QuoteTask(host, port, 2, symbol));
        RUNNABLE_MAP.put(4, new QuoteTask(host, port, 4, symbol));
        RUNNABLE_MAP.put(8, new QuoteTask(host, port, 8, symbol));

        if (future != null) {
            future.cancel(true);
            future = null;
        }

        List<Runnable> rl = new ArrayList<>();
        for (Map.Entry<Integer, Runnable> integerRunnableEntry : RUNNABLE_MAP.entrySet()) {
            Integer k = integerRunnableEntry.getKey();
            if ((type & k) == k) {
                rl.add(integerRunnableEntry.getValue());
                log.info("Add task [{}] to loop", k);
            }
        }

        future = scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                log.info("start query");
                for (int i = 0; i < qps; ++i) {
                    executor.submit(rl.get(i % rl.size()));
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 1, 1, TimeUnit.SECONDS);

        return "OK";
    }

    @GetMapping("/stop")
    public String stop() {
        executor.shutdown();
        executor = null;
        future.cancel(true);
        future = null;
        return "OK";
    }

    static OkHttpClient client = new OkHttpClient.Builder()
        .connectionPool(new ConnectionPool(50, 5, TimeUnit.SECONDS))
        .build();

    public static class QuoteTask implements Runnable {
        private String url;
        private int type;

        public QuoteTask(String host, int port, int type, String symbol) {
            this.url = buildUrl(host, port, type, symbol);
            this.type = type;
        }

        @Override
        public void run() {
            Request request = new Builder().url(url)
                .header("Host", "www.bhex.us")
                .build();
            long start = System.currentTimeMillis();
            try {
                Response response = client.newCall(request)
                    .execute();
                int code = response.code();
                response.close();
                metricsLatency(this.type, code, start);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                metricsLatency(this.type, 500, start);
            }
        }

        // type
        // 0     0      0      0
        // kline trade depth ticker
        private String buildUrl(String host, int port, int type, String symbol) {
            StringBuilder sb = new StringBuilder();
            sb.append("http://").append(host).append(":").append(port).append("/");
            if (type == 1) {
                sb.append("openapi/quote/v1/ticker/24hr?symbol=");
            } else if (type == 2) {
                sb.append("openapi/quote/v1/depth?symbol=");
            } else if (type == 4) {
                sb.append("openapi/quote/v1/trades?symbol=");
            } else if (type == 8) {
                sb.append("openapi/quote/v1/klines?interval=1m&symbol=");
            }
            return sb.append(symbol).toString();
        }
    }


    private static void metricsLatency(int type, int code, long start) {
        requestLatency
            .labels(String.valueOf(type), String.valueOf(code))
            .observe(System.currentTimeMillis() - start);
    }
}
