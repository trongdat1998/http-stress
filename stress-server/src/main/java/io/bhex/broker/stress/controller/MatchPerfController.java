/**********************************
 * @项目名称: stress-parent
 * @文件名称: MatchPerfController
 * @Date 2020-01-10
 * @Author fulintang
 * @Copyright（C）: 2018 BlueHelix Inc. All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/


package io.bhex.broker.stress.controller;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bhex.base.match.CancelMatchOrderRequest;
import io.bhex.base.match.NewMatchOrderRequest;
import io.bhex.base.metrics.PrometheusMetricsCollector;
import io.bhex.base.proto.Decimal;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.proto.OrderTimeInForceEnum;
import io.bhex.base.proto.OrderTypeEnum;
import io.bhex.base.protocol.exchange.TradeResponse;
import io.bhex.base.protocol.exchange.TradeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * for local test in IDEA:
 * <p>
 * -Dspring.profiles.active=cn -Denv.profile=test -Xmx8g -Xms8g -XX:MaxDirectMemorySize=512M -XX:+UseG1GC -XX:MaxGCPauseMillis=50 -XX:G1HeapWastePercent=20 -XX:+PrintAdaptiveSizePolicy -XX:+UnlockCommercialFeatures -XX:+FlightRecorder
 */

@Slf4j
//@RestController
//@RequestMapping("/internal/match_perf")
public class MatchPerfController {

    static int exchangeId = 301;
    static int brokerId = 6002;

    static ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("perf-%d").build();
    static ThreadPoolExecutor executor;

    static int logPerRequest = 200_000;

    static AtomicLong totalOrderNumber = new AtomicLong();
    static AtomicLong totalSuccPushOrderNumber = new AtomicLong();
    static AtomicLong totalFailPushOrderNumber = new AtomicLong();
    static AtomicLong totalSuccCancelOrderNumber = new AtomicLong();
    static AtomicLong totalFailCancelOrderNumber = new AtomicLong();

    static long taskStartTime;

    ScheduledExecutorService staticThread = Executors.newScheduledThreadPool(1);

    static boolean stop = false;

    static final Histogram requestLatency = Histogram.build()
            .name("match_requests_latency_milliseconds")
            .help("Request latency in milliseconds.")
            .labelNames("symbol", "method", "result")
            .buckets(PrometheusMetricsCollector.RPC_TIME_BUCKETS)
            .register();

    /**
     * curl "http://127.0.0.1:7222/internal/match_perf/start?grpcServer=match-engine-1.exchange&grpcPort=7040&startIdx=1410&nThreads=2&nTradeThreads=1&grpcCall=10&symbol=ETHUSDT"
     *
     * @param startIdx      0... 1999 user index
     * @param nThreads      开启 nThreads 个线程
     * @param nTradeThreads 其中 nTradeThreads (0 ~ nThreads) 做成交，其它的仅下单撤单
     * @param grpcCall      grpc call number for waiting response
     * @param symbol
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/start")
    public Boolean startPerfTest(@RequestParam(defaultValue = "127.0.0.1") String grpcServer,
                                 @RequestParam(defaultValue = "7040") Integer grpcPort,
                                 @RequestParam(defaultValue = "1110") Integer startIdx,
                                 @RequestParam(defaultValue = "10") Integer nThreads,
                                 @RequestParam(defaultValue = "2") Integer nTradeThreads,
                                 @RequestParam(defaultValue = "4") Integer grpcCall,
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

        taskStartTime = System.currentTimeMillis();
        totalOrderNumber.set(0L);
        totalSuccPushOrderNumber.set(0L);
        totalFailPushOrderNumber.set(0L);
        totalSuccCancelOrderNumber.set(0L);
        totalFailCancelOrderNumber.set(0L);

        for (int i = startIdx; i < startIdx + nThreads; i++) {
            boolean trade = false;
            if (i < startIdx + nTradeThreads) {
                trade = true;
            }
            PerfTask task = new PerfTask(grpcServer, grpcPort, trade, i, symbol, grpcCall);
            executor.submit(task);
        }

        staticThread.scheduleWithFixedDelay(() -> {
            long elapseTime = System.currentTimeMillis() - taskStartTime;
            double qps = totalSuccPushOrderNumber.get() * 1000.0 / elapseTime;
            log.info("staticThread: qps {} elapseTime {} \n succ_push {} fail_push {} succ_cancel {} fail_cancel {}",
                    qps, elapseTime,
                    totalSuccPushOrderNumber.getAndSet(0),
                    totalFailPushOrderNumber.getAndSet(0),
                    totalSuccCancelOrderNumber.getAndSet(0),
                    totalFailCancelOrderNumber.getAndSet(0));
            taskStartTime = System.currentTimeMillis();
        }, 10, 10, TimeUnit.SECONDS);

        return true;
    }

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

    static class PerfTask implements Runnable {
        String grpcServer = "127.0.0.1";
        int grpcPort = 7040;

        boolean trade;
        int idx;
        int grpcCall;
        String idxSuffix;
        String symbol;
        ManagedChannel tradeChannel;
        TradeServiceGrpc.TradeServiceStub tradeServiceStub;

        ArrayBlockingQueue<Long> orders = new ArrayBlockingQueue<>(655360);
        ExecutorService cancelThread = Executors.newFixedThreadPool(1);

        Semaphore maxOutstanding;
        AtomicLong cancelNumber = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        PerfTask(String grpcServer, int grpcPort, boolean trade, int idx, String symbol, int grpcCall) {
            this.grpcServer = grpcServer;
            this.grpcPort = grpcPort;
            this.idx = idx;
            this.grpcCall = grpcCall;
            this.symbol = symbol;
            this.trade = trade;
            this.idxSuffix = String.format("%04d", idx);

            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                    .forAddress(grpcServer, grpcPort)
                    .usePlaintext();

            tradeChannel = channelBuilder.build();
            tradeServiceStub = TradeServiceGrpc.newStub(tradeChannel);
            maxOutstanding = new Semaphore(grpcCall);

            log.info("start task idx {}, trade {}, symbol {}, grpcCall {}", idx, trade, symbol, grpcCall);
        }

        @Override
        public void run() {
            cancelThread.submit(() -> {
                log.info("starting cancelThread for idx {}", idx);
                try {
                    while (true) {
                        cancelOrders();
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    log.warn("cancelThread stop because {}", e.getMessage());
                }
            });

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                if (stop) {
                    cancelThread.shutdown();
                    break;
                }

                try {
                    startTime = System.currentTimeMillis();
                    placeOrder(i, i % logPerRequest == 0);
                } catch (Exception e) {
                    log.error("exception: {}", e.getMessage());
                    metricsRecode(startTime, "placeOrder", "fail");
                }
            }
        }

        private void cancelOrders() throws InterruptedException {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                Long orderId = orders.poll(10, TimeUnit.MILLISECONDS);
                if (orderId == null) {
                    return;
                }

                cancelOrder(orderId, cancelNumber.incrementAndGet() % logPerRequest == 0);
            }
        }

        void placeOrder(int num, boolean doLog) {
            int amount = num + idx;
            int price = num + idx;
            int quantity = num + idx;
            maxOutstanding.acquireUninterruptibly();

            NewMatchOrderRequest newOrderRequest = buildNewOrderRequest(num, amount, price, quantity, OrderSideEnum.BUY);
            tradeServiceStub.pushOrder(newOrderRequest, new StreamObserver<TradeResponse>() {
                long startTime = System.currentTimeMillis();
                long orderId;

                @Override
                public void onNext(TradeResponse value) {
                    if (doLog) {
                        log.info("pushOrder onNext: order id {}, seqId {}", value.getOrderId(), value.getSequenceId());
                    }
                    orderId = value.getOrderId();
                }

                @Override
                public void onError(Throwable t) {
                    log.warn("pushOrder onError: {}", t.getMessage());
                    metricsRecode(startTime, "pushOrder", "fail");
                    maxOutstanding.release();
                    totalFailPushOrderNumber.incrementAndGet();
                }

                @Override
                public void onCompleted() {
                    metricsRecode(startTime, "pushOrder", "succ");
                    cancelOrder(orderId, doLog);
                    maxOutstanding.release();
                    totalSuccPushOrderNumber.incrementAndGet();
                }
            });
        }

        void cancelOrder(long orderId, boolean doLog) {
            CancelMatchOrderRequest.Builder builder = CancelMatchOrderRequest.newBuilder();
            builder.setExchangeId(exchangeId)
                    .setSymbolId(symbol)
                    .setSymbolName(symbol)
                    .setOrderId(orderId)
                    .setAccountId(100_000_000L + idx)
                    .setCreatedTime(System.currentTimeMillis());

            tradeServiceStub.cancelOrder(builder.build(), new StreamObserver<TradeResponse>() {
                long startTime = System.currentTimeMillis();

                @Override
                public void onNext(TradeResponse value) {
                    if (doLog) {
                        log.info("cancelOrder onNext: orderId {}, bizCode {}", value.getOrderId(), value.getBizCodeValue());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.warn("cancelOrder onError: {}", t.getMessage());
                    metricsRecode(startTime, "cancelOrder", "fail");
                    totalFailCancelOrderNumber.incrementAndGet();
                }

                @Override
                public void onCompleted() {
                    metricsRecode(startTime, "cancelOrder", "succ");
                    totalSuccCancelOrderNumber.incrementAndGet();
                }
            });
        }

        Decimal minimum = Decimal.newBuilder().setStr("0.0001").setScale(8).build();
        Decimal feeRate = Decimal.newBuilder().setStr("0.001").setScale(8).build();

        private NewMatchOrderRequest buildNewOrderRequest(int num, int amount, int price, int quantity,
                                                          OrderSideEnum side) {
            NewMatchOrderRequest.Builder builder = NewMatchOrderRequest.newBuilder();
            builder.setAccountId(100_000_000L + idx)
                    .setBrokerId(brokerId)
                    .setExchangeId(exchangeId)
                    .setOrderId(idx * 1_000_000_000_000_000L + num)
                    .setSymbolId(symbol)
                    .setSymbolName(symbol)
                    .setAmount(Decimal.newBuilder().setStr(String.valueOf(amount * 1.0d / 100)).setScale(8).build())
                    .setPrice(Decimal.newBuilder().setStr(String.valueOf(price * 1.0d / 100)).setScale(8).build())
                    .setQuantity(Decimal.newBuilder().setStr(String.valueOf(quantity * 1.0d / 100)).setScale(8).build())
                    .setMinimumQuantity(minimum)
                    .setMinimumAmount(minimum)
                    .setMakerFeeRate(feeRate)
                    .setTakerFeeRate(feeRate)
                    .setOrderType(OrderTypeEnum.LIMIT)
                    .setSide(side)
                    .setCreatedTime(System.currentTimeMillis())
                    .setTimeInForce(OrderTimeInForceEnum.GTC);

            return builder.build();
        }

        private void metricsRecode(long startTime, String method, String result) {
            long cost = System.currentTimeMillis() - startTime;
            requestLatency.labels(symbol, method, result).observe(cost);
        }
    }
}
