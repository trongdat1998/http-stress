package io.bhex.broker.stress.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bhex.base.account.OrderServiceGrpc;
import io.bhex.base.proto.OrderSideEnum;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okhttp3.internal.http.HttpCodec;
import okio.ByteString;
import okio.Sink;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@RestController
@RequestMapping("/internal/openapi_perf")
public class OpenapiPerfController {

    static ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("perf-%d").build();
    static ThreadPoolExecutor executor;

    public static final Histogram requestLatency = Histogram.build()
            .name("openapi_requests_latency_milliseconds")
            .help("Request latency in milliseconds.")
            .labelNames("symbol", "method", "result")
            .buckets(new double[]{0.5D, 1D, 2D, 3D, 4D, 5D, 6D, 7D, 8D, 10D, 20D, 30D, 50D, 100D, 1000D, 10000D})
            .register();

    public static final Histogram orderPushLatency = Histogram.build()
            .name("openapi_push_latency_milliseconds")
            .help("Order receive delay in milliseconds.")
            .labelNames("symbol", "method", "status")
            .buckets(new double[]{0.5D, 1D, 2D, 3D, 4D, 5D, 6D, 7D, 8D, 10D, 20D, 30D, 50D, 100D, 1000D, 10000D})
            .register();

    static int logPerRequest = 2000;

    static boolean stop = false;

    static ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

    @PostConstruct
    public void init() {
        pool.scheduleAtFixedRate(() -> {
            if (client.connectionPool().connectionCount() > 0) {
                log.info("connPool: {}, idle: {}", client.connectionPool().connectionCount(), client.connectionPool().idleConnectionCount());
            }
            if (counter.get() > 0) {
                log.info("avgTime: {}, count: {}", avgTime, counter.get());
            }
        }, 1, 15, TimeUnit.SECONDS);
    }

    static long beginMetrics = 0;

    /**
     * @param grpcServer
     * @param grpcPort
     * @param startAid
     * @param count
     * @param nThreads
     * @param nTradeThreads
     * @param basePrice     基础价格线，根据当前价格用于下单价格计算
     * @param symbol
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/start")
    public Boolean startPerfTest(@RequestParam(defaultValue = "127.0.0.1") String grpcServer,
                                 @RequestParam(defaultValue = "7011") Integer grpcPort,
                                 @RequestParam(defaultValue = "130001") Integer startAid,
                                 @RequestParam(defaultValue = "2000") Integer count,
                                 @RequestParam(defaultValue = "10") Integer nThreads,
                                 @RequestParam(defaultValue = "2") Integer nTradeThreads,
                                 @RequestParam(defaultValue = "1000") BigDecimal basePrice,
                                 @RequestParam(defaultValue = "ETHUSDT") String symbol) throws Exception {
        if (executor != null) {
            stop = true;
            stopAllTask();
        }

        stop = false;
        executor = new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(nThreads * 2),
                factory, new ThreadPoolExecutor.AbortPolicy());
        executor.prestartAllCoreThreads();

        //每个线程使用的账户数量
        int aidCount = count / nThreads;

        for (int i = 0; i < nThreads; i++) {
            PerfTask task = new PerfTask(grpcServer, grpcPort, 6001l, count * nTradeThreads / nThreads / nThreads, startAid + aidCount * i, aidCount, symbol, basePrice);
            executor.submit(task);
        }

        beginMetrics = System.currentTimeMillis();

        if (staticTask != null) {
            staticTask.cancel(true);
            staticTask = null;
        }
        return true;
    }

    ScheduledFuture staticTask;

    /**
     * curl "http://127.0.0.1:7222/internal/match_perf/stop"
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/stop")
    public Boolean stopPerfTest() throws Exception {
        stop = true;
        return stopAllTask();
    }

    private boolean stopAllTask() {
        if (!executor.isShutdown()) {
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("executor awaitTermination error {}", e.getMessage());
            }
        }
        return true;
    }


    LinkedList<WebSocket> webSockets = new LinkedList<>();

    AtomicLong counter = new AtomicLong(0);

    double avgTime = 0.0;

    @RequestMapping(value = "/listen")
    public Boolean listenWs(@RequestParam(defaultValue = "127.0.0.1") String server,
                            @RequestParam(defaultValue = "7011") Integer port,
                            @RequestParam(defaultValue = "130001") Long start,
                            @RequestParam(defaultValue = "160000") Long end,
                            @RequestParam(defaultValue = "300") Integer count
    ) throws Exception {
        synchronized (webSockets) {
            while (!webSockets.isEmpty()) {
                WebSocket webSocket = webSockets.remove();
                webSocket.close(1000, "close");
            }
            for (Long accountId = start; accountId <= end; accountId += count) {
                Long endAccountId = accountId + count - 1;
                if (endAccountId > end) {
                    endAccountId = end;
                }
                String listenKey = "perf_test-" + accountId + "-" + (endAccountId);
                Request request = new Request.Builder().url("http://" + server + ":" + port + "/openapi/ws/" + listenKey).build();
                log.info("ListenKey: {}", listenKey);
                WebSocket webSocket = client.newWebSocket(request, new WebSocketListener() {
                    @Override
                    public void onMessage(WebSocket webSocket, ByteString bytes) {
                        super.onMessage(webSocket, bytes);
                    }

                    @Override
                    public void onClosed(WebSocket webSocket, int code, String reason) {
                        log.info("closed: {}", webSocket);
                    }

                    @Override
                    public void onMessage(WebSocket webSocket, String text) {
                        if (text.startsWith("[")) {
                            JSONArray array = JSONArray.parseArray(text);
                            int size = array.size();
                            for (int i = 0; i < size; i++) {
                                JSONObject object = array.getJSONObject(i);
                                String eventType = object.getString("e");
                                String symbol = object.getString("s");
                                //订单
                                if (eventType.equalsIgnoreCase("executionReport")) {
                                    counter.incrementAndGet();
                                    String clientOrderId = object.getString("c");
                                    String[] arr = clientOrderId.split("-");
                                    Long st = Long.valueOf(arr[1]);
                                    Long cost = System.currentTimeMillis() - st;
                                    avgTime = cost;
                                    orderPushLatency.labels(symbol, eventType, object.getString("X")).observe(cost);
                                } else if (eventType.equalsIgnoreCase("ticketInfo")) {
                                    Long cost = System.currentTimeMillis() - Long.valueOf(object.getLong("t"));
                                    orderPushLatency.labels(symbol, eventType, "MATCHED").observe(cost);
                                } else if (eventType.equalsIgnoreCase("outboundAccountInfo")) {
                                    Long cost = System.currentTimeMillis() - Long.valueOf(object.getLong("E"));
                                    orderPushLatency.labels("", eventType, "Update").observe(cost);
                                }
                            }
                        } else {
                            JSONObject obj = JSONObject.parseObject(text);
                            if (obj.getLong("ping") != null) {
                                Long ping = obj.getLong("ping");
                                log.info("diff: {}", System.currentTimeMillis() - ping);
                            }
                            log.info("recv: {}", text);
                        }
                    }
                });
                webSockets.add(webSocket);
            }
        }
        return true;
    }

    @RequestMapping(value = "/close")
    public Boolean stopWs() {
        synchronized (webSockets) {
            while (!webSockets.isEmpty()) {
                WebSocket webSocket = webSockets.remove();
                webSocket.close(1000, "close");
            }
        }
        return true;
    }


    static Dns cachedSystemDns = new Dns() {
        Cache<String, List<InetAddress>> dnsCache = CacheBuilder
                .newBuilder()
                .maximumSize(100)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .build();

        @Override
        public List<InetAddress> lookup(String hostname) throws UnknownHostException {
            if (hostname == null) throw new UnknownHostException("hostname == null");

            try {
                List<InetAddress> cachedAddress = dnsCache.get(hostname, () -> Arrays.asList(InetAddress.getAllByName(hostname)));

                return cachedAddress;
            } catch (ExecutionException e) {
                UnknownHostException unknownHostException =
                        new UnknownHostException("Broken system behaviour for dns lookup of " + hostname);
                unknownHostException.initCause(e);
                throw unknownHostException;
            }
        }
    };

    static ExecutorService clientExecutor = Executors.newFixedThreadPool(30);

    static OkHttpClient client;

    static {
        client = new OkHttpClient.Builder()
                .pingInterval(20, TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(50, 1, TimeUnit.SECONDS))
                .connectTimeout(1, TimeUnit.SECONDS)
                .readTimeout(1, TimeUnit.SECONDS)
                .writeTimeout(1, TimeUnit.SECONDS)
                .callTimeout(1, TimeUnit.SECONDS)
                .build();
    }

    static class PerfTask implements Runnable {
        //成交订单占比
        int tradeCount;
        int startAid;
        int count;
        Long orgId;
        String symbol;
        BigDecimal basePrice;

        long startTime = System.currentTimeMillis();

        long beginId = 1615252523000L;

        BigDecimal priceDiff;

        String url;

        PerfTask(String grpcServer, int grpcPort, Long orgId, int tradeCount, int startAid, int count, String symbol, BigDecimal basePrice) {
            this.basePrice = basePrice;
            this.startAid = startAid;
            this.symbol = symbol;
            this.tradeCount = tradeCount;
            //必须是4倍数
            if (count % 4 != 0) {
                count -= count % 4;
            }
            this.count = count;
            this.orgId = orgId;

            this.priceDiff = basePrice.divide(new BigDecimal("2").multiply(new BigDecimal(count)), RoundingMode.DOWN);
            this.url = "http://" + grpcServer + ":" + grpcPort;

            log.info("start task, startAid {},count {}, tradeCount {}, symbol {}", startAid, count, tradeCount, symbol);
        }

        List<JSONObject> orders = new LinkedList<>();
        OrderServiceGrpc.OrderServiceBlockingStub orderServiceBlockingStub;

        @Override
        public void run() {
            boolean reverse = false;
            int loopCount = 0;
            int halfTrade = count / 2;
            //循环下单
            while (true) {
                try {
                    if (stop) {
                        break;
                    }
                    startTime = System.currentTimeMillis();
                    orders = new LinkedList<>();

                    updateDelay();

                    long offset = 2 * ((loopCount / 40) % halfTrade);
                    offset = offset % halfTrade;
                    long i = 0;
                    //每10次成交一次
                    boolean isBuy = loopCount % 20 == 18;
                    boolean isSell = loopCount % 20 == 19;
                    //前一半负责下成交单，但是10次循环执行一次
                    if (isBuy) {
                        //先下单
                        for (i = offset; i < halfTrade + offset; i++) {
                            if (reverse) {
                                dealOrder(this.startAid + i, OrderSideEnum.BUY, true);
                            } else {
                                dealOrder(this.startAid + i, OrderSideEnum.SELL, true);
                            }
                        }
                    } else if (isSell) {
                        //先下单
                        for (i = offset; i < halfTrade + offset; i++) {
                            if (reverse) {
                                dealOrder(this.startAid + i, OrderSideEnum.SELL, true);
                            } else {
                                dealOrder(this.startAid + i, OrderSideEnum.BUY, true);
                            }
                        }
                        reverse = !reverse;
                    } else {
                        for (i = 0; i < offset; i += 2) {
                            dealOrder(this.startAid + i, OrderSideEnum.SELL, false);
                            dealOrder(this.startAid + i + 1, OrderSideEnum.BUY, false);
                        }
                        //先下单
                        for (i = halfTrade + offset; i < count; i += 2) {
                            dealOrder(this.startAid + i, OrderSideEnum.SELL, false);
                            dealOrder(this.startAid + i + 1, OrderSideEnum.BUY, false);
                        }
                    }
                    loopCount++;
                    long cost = System.currentTimeMillis() - startTime;
                    log.info("dealOrder cost: {}, size: {}", cost, halfTrade);
                    startTime = System.currentTimeMillis();
                    cancelOrders();
                    cost = System.currentTimeMillis() - startTime;
                    log.info("cancelOrders cost: {}, size: {}", cost, orders.size());
                } catch (Exception e) {
                    log.error("exception: {}", e.getMessage(), e);
                }
            }
        }

        protected long delayTime1 = 0l;
        protected long delayTime2 = 0l;

        void updateDelay() {
            try {
                long st = System.currentTimeMillis();
                Request request = new Request.Builder()
                        .url(url + "/openapi/time")
                        .post(RequestBody.create(MediaType.parse("application/json"), "{}"))
                        .build();
                Call call = client.newCall(request);
                String resp = call.execute().body().string();
                JSONObject obj = JSONObject.parseObject(resp);
                delayTime1 = obj.getLong("serverTime") - st;
                metricsRecode2(delayTime1, "delayTime", "send");
                delayTime2 = System.currentTimeMillis() - obj.getLong("serverTime");
                metricsRecode2(delayTime2, "delayTime", "receive");
                metricsRecode2(System.currentTimeMillis() - st, "delayTime", "total");
                log.info("delay: {},{},cost: {}", delayTime1, delayTime2, System.currentTimeMillis() - st);
            } catch (Exception e) {

            }
        }


        void dealOrder(Long aid, OrderSideEnum side, boolean isTrade) {
//            log.info("aid: {}, side: {}, isTrade: {}", aid, side, isTrade);
            String url = buildNewOrderRequest(aid, side, isTrade);
            long startTime = System.currentTimeMillis();
            try {
                JSONObject resp1 = sendReq(url, aid + 100000, aid);
                long cost = System.currentTimeMillis() - startTime;
                if (!isTrade && resp1 != null) {
                    orders.add(resp1);
                }
                if (resp1 == null) {
                    log.warn("cancel ret is null: aid={}", aid);
                    metricsRecode(startTime, "pushOrder", "pushFail");
                } else {
                    if (resp1.getString("code") != null) {
                        log.error("order error: {}", resp1);
                        metricsRecode(startTime, "pushOrder", resp1.getString("code"));
                    } else if (resp1.getString("status") != null) {
                        metricsRecode(startTime, "pushOrder", resp1.getString("status"));
                    } else {
                        log.error("order resp: {}", resp1);
                        metricsRecode(startTime, "pushOrder", "pushFail");
                    }
                }
                if (!isTrade) {
                    if (cost < 10) {
                        Thread.sleep(10 - cost);
                    }
                } else {
                    if (cost < 50) {
                        Thread.sleep(50 - cost);
                    }
                }
            } catch (Exception e) {
                log.warn("pushOrder onError: " + e.getMessage(), e);
                metricsRecode(startTime, "pushOrder", e.getMessage() == null ? "null" : e.getMessage());
            }
        }

        private void cancelOrders() {
            //一个个去撤单
            for (JSONObject order : orders) {
                long startTime = System.currentTimeMillis();
                try {
                    JSONObject resp = sendDeleteReq("/openapi/account/order?symbol=" + symbol + "&fastCancel=1&symbolId=" + symbol + "&orderId=" + order.getString("orderId") + "&clientOrderId=" + order.getString("clientOrderId"), 100000 + order.getLong("accountId"), order.getLong("accountId"));
                    long cost = System.currentTimeMillis() - startTime;
                    if (resp != null) {
                        if (resp.getString("code") != null) {
                            if (resp.getInteger("code") == -1139) {
                                metricsRecode(startTime, "cancelOrder", "FILLED");
                            } else {
                                log.info("cancel error: {}", resp);
                                metricsRecode(startTime, "cancelOrder", resp.getString("msg"));
                            }
                        } else {
                            metricsRecode(startTime, "cancelOrder", "succ");
                        }
                    } else {
                        log.warn("cancel ret is null: orderId={}, aid={}", order.getString("orderId"), order.getString("accountId"));
                        metricsRecode(startTime, "cancelOrder", "CancelFail");
                    }
                    if (cost < 10) {
                        Thread.sleep(10 - cost);
                    }
                } catch (Exception e) {
                    log.warn("cancelOrder onError: {}", e.getMessage(), e);
                    metricsRecode(startTime, "cancelOrder", e.getMessage() == null ? "null" : e.getMessage());
                }
            }
        }

        JSONObject sendDeleteReq(String path, Long userId, Long accountId) throws IOException {
            Request request = new Request.Builder()
                    .addHeader("X-BH-APIKEY", "tmcqQ1ecjwTniAEPGgpEwBBH3Y2xcgQFqeCCewKPz4ki4uuE1WkIYKp9Zi15HofU")
                    .addHeader("user_id", userId + "")
                    .addHeader("account_id", accountId + "")
                    .url(url + path)
                    .delete()
                    .build();
            Call call = client.newCall(request);
            return JSONObject.parseObject(call.execute().body().string());
        }

        JSONObject sendReq(String path, Long userId, Long accountId) throws IOException {
            Request request = new Request.Builder()
                    .addHeader("X-BH-APIKEY", "tmcqQ1ecjwTniAEPGgpEwBBH3Y2xcgQFqeCCewKPz4ki4uuE1WkIYKp9Zi15HofU")
                    .addHeader("user_id", userId + "")
                    .addHeader("account_id", accountId + "")
                    .url(url + path)
                    .post(RequestBody.create(MediaType.parse("application/json"), "{}"))
                    .build();
            Call call = client.newCall(request);
            String resp = call.execute().body().string();
            return JSONObject.parseObject(resp);
        }

        private String buildNewOrderRequest(Long accountId,
                                            OrderSideEnum side, boolean isTrade) {

            BigDecimal step = new BigDecimal("0.05").multiply(new BigDecimal(accountId - startAid));

            //默认买卖同价，确保成交
            BigDecimal price;
            if (!isTrade) {
                if (side == OrderSideEnum.BUY) {
                    price = basePrice.subtract(step);
                } else {
                    price = basePrice.add(step);//逐步升高
                }
            } else {
                if (side == OrderSideEnum.BUY) {
                    price = basePrice.add(new BigDecimal("0.02"));//逐步升高
                } else {
                    price = basePrice.subtract(new BigDecimal("0.02"));
                }
            }
            String clientOrderId = accountId + "-" + System.currentTimeMillis();
            return "/openapi/order?timestamp=" + System.currentTimeMillis() + "&signature=1&symbol=" + symbol + "&side=" + side + "&type=LIMIT&quantity=0.1&price=" + price + "&newClientOrderId=" + clientOrderId;
        }

        private void metricsRecode2(long cost, String method, String result) {
            //前15秒预热
            if (System.currentTimeMillis() - beginMetrics < 15000) {
                return;
            }
            if (cost > 2) {
                log.warn("delay > 2: {},result:{}", method, result);
                return;
            }
            requestLatency.labels(symbol, method, result).observe(cost);
        }

        private void metricsRecode(long startTime, String method, String result) {
            //前15秒预热
            if (startTime - beginMetrics < 15000) {
                return;
            }
            long cost = System.currentTimeMillis() - startTime;
            requestLatency.labels(symbol, method, result).observe(cost);
        }

    }

    static class HttpCodecWrapper implements HttpCodec {
        private HttpCodec codec;

        public HttpCodecWrapper(HttpCodec codec) {
            this.codec = codec;
        }

        @Override
        public Sink createRequestBody(Request request, long l) {
            return codec.createRequestBody(request, l);
        }

        @Override
        public void writeRequestHeaders(Request request) throws IOException {
            codec.writeRequestHeaders(request);
        }

        @Override
        public void flushRequest() throws IOException {
            codec.flushRequest();
        }

        @Override
        public void finishRequest() throws IOException {
            codec.finishRequest();
        }

        @Override
        public Response.Builder readResponseHeaders(boolean expectContinue) throws IOException {
            return codec.readResponseHeaders(expectContinue).addHeader("Connection", "keep-alive");
        }

        @Override
        public ResponseBody openResponseBody(Response response) throws IOException {
            return codec.openResponseBody(response);
        }

        @Override
        public void cancel() {
            codec.cancel();
        }
    }
}
