package io.bhex.broker.stress.controller;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bhex.base.match.CancelMatchOrderRequest;
import io.bhex.base.match.NewMatchOrderRequest;
import io.bhex.base.metrics.PrometheusMetricsCollector;
import io.bhex.base.proto.*;
import io.bhex.base.protocol.exchange.TradeResponse;
import io.bhex.base.protocol.exchange.TradeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
@RestController
@RequestMapping("/internal/match_perf")
public class NewMatchPerfController {

    static int exchangeId = 301;
    static int brokerId = 6002;

    static ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("perf-%d").build();
    static ThreadPoolExecutor executor;

    static int logPerRequest = 200_000;

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

        taskStartTime = System.currentTimeMillis();

        //每个线程使用的账户数量
        int aidCount = count / nThreads;

        for (int i = 0; i < nThreads; i++) {
            PerfTask task = new PerfTask(grpcServer, grpcPort, 6001l, count * nTradeThreads / nThreads / nThreads, startAid + aidCount * i, aidCount, symbol, basePrice);
            executor.submit(task);
        }

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
        //成交订单占比
        int tradeCount;
        boolean trade;
        int startAid;
        int count;
        Long orgId;
        String symbol;
        BigDecimal basePrice;
        ManagedChannel tradeChannel;

        long startTime = System.currentTimeMillis();
        long beginId = 1615252523000L;

        BigDecimal priceDiff;

        PerfTask(String grpcServer, int grpcPort, Long orgId, int tradeCount, int startAid, int count, String symbol, BigDecimal basePrice) {
            this.basePrice = basePrice;
            this.startAid = startAid;
            this.symbol = symbol;
            this.tradeCount = tradeCount;
            this.count = count;
            this.orgId = orgId;

            this.priceDiff = basePrice.divide(new BigDecimal("2").multiply(new BigDecimal(count)), RoundingMode.DOWN);

            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                    .forAddress(grpcServer, grpcPort)
                    .usePlaintext();
            tradeChannel = channelBuilder.build();
            log.info("start task, startAid {},count {}, trade {}, symbol {}", startAid, count, trade, symbol);
        }

        TradeServiceGrpc.TradeServiceBlockingStub tradeServiceBlockingStub;

        List<TradeResponse> orders;

        @Override
        public void run() {
            boolean reverse = false;
            //循环下单
            while (true) {
                try {
                    if (stop) {
                        break;
                    }
                    //每次买卖双方的人互换，避免挂掉
                    reverse = !reverse;
                    startTime = System.currentTimeMillis();
                    orders = new LinkedList<>();
                    tradeServiceBlockingStub = TradeServiceGrpc.newBlockingStub(tradeChannel);

                    long i = 0;
                    //先下单
                    for (; i < count; i += 2) {
                        if (reverse) {
                            dealOrder(this.startAid + i, OrderSideEnum.BUY, i < tradeCount);
                            dealOrder(this.startAid + i + 1, OrderSideEnum.SELL, i < tradeCount);
                        } else {
                            dealOrder(this.startAid + i, OrderSideEnum.SELL, i < tradeCount);
                            dealOrder(this.startAid + i + 1, OrderSideEnum.BUY, i < tradeCount);
                        }
                    }
                    cancelOrders();
                    //等待10毫秒秒
                    Thread.sleep(10);
                } catch (Exception e) {
                    log.error("exception: {}", e.getMessage(), e);
                }
            }
        }

        void dealOrder(Long aid, OrderSideEnum side, boolean isTrade) {
            NewMatchOrderRequest newOrderRequest1 = buildNewOrderRequest(aid, side, isTrade);
            try {
                startTime = System.currentTimeMillis();
                TradeResponse resp1 = tradeServiceBlockingStub.pushOrder(newOrderRequest1);
                if (!isTrade) {
                    orders.add(resp1);
                }
                if (resp1.getFinishedCount() > 0) {
                    metricsRecode(startTime, "pushOrder", "" + resp1.getFinished(0).getOrderStatus());
                } else {
                    metricsRecode(startTime, "pushOrder", "" + resp1.getCode());
                }
            } catch (Exception e) {
                log.warn("pushOrder onError: {}", e.getMessage());
                metricsRecode(startTime, "pushOrder", e.getMessage());
            }
        }

        void cancelOrders() {
            for (TradeResponse response : orders) {
                try {
                    startTime = System.currentTimeMillis();
                    TradeResponse reply1 = tradeServiceBlockingStub.cancelOrder(CancelMatchOrderRequest.newBuilder()
                            .setExchangeId(301l)
                            .setSymbolId(symbol)
                            .setSymbolName(symbol)
                            .setAccountId(response.getAccountId())
                            .setOrderId(response.getOrderId())
                            .setCreatedTime(System.currentTimeMillis())
                            .build());
                    if (!trade) { //不成交测试的测单结果
                        metricsRecode(startTime, "cancelOrder", reply1.getCode() + "");
//                    } else {
//                        if (reply1.getCode() == -1) {
//                            metricsRecode(startTime, "strikeOrder", "filled");
//                        } else {
//                            metricsRecode(startTime, "strikeOrder", "canceled");
//                        }
                    }
                } catch (Exception e) {
                    log.warn("cancelOrder onError: {}", e.getMessage());
                    metricsRecode(startTime, "cancelOrder", e.getMessage());
                }
            }
        }

        Decimal minimum = Decimal.newBuilder().setStr("0.0001").setScale(8).build();
        Decimal feeRate = Decimal.newBuilder().setStr("0.001").setScale(8).build();

        private NewMatchOrderRequest buildNewOrderRequest(Long accountId,
                                                          OrderSideEnum side, boolean isTrade) {
            BigDecimal step = priceDiff.multiply(new BigDecimal(accountId - startAid));

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
                    price = basePrice.add(priceDiff.divide(new BigDecimal("2"), RoundingMode.DOWN));//逐步升高
                } else {
                    price = basePrice.subtract(priceDiff.divide(new BigDecimal("2"), RoundingMode.DOWN));
                }
            }
//            log.info("aid:{},price:{},side:{}", accountId, price, side);
            int rand = RandomUtils.nextInt(1000, 9999);
            BigDecimal qty = new BigDecimal("0.01");
            return NewMatchOrderRequest.newBuilder()
                    .setAccountId(accountId)
                    .setQuantity(DecimalUtil.fromBigDecimal(qty))
                    .setPrice(DecimalUtil.fromBigDecimal(price))
                    .setAmount(DecimalUtil.fromBigDecimal(qty.multiply(price)))
                    .setSymbolId(symbol)
                    .setSymbolName(symbol)
                    .setCreatedTime(System.currentTimeMillis())
                    .setOrderType(OrderTypeEnum.LIMIT)
                    .setBrokerId(orgId)
                    .setMinimumQuantity(minimum)
                    .setMinimumAmount(minimum)
                    .setMakerFeeRate(feeRate)
                    .setTakerFeeRate(feeRate)
                    .setTimeInForce(OrderTimeInForceEnum.GTC)
                    .setSide(side)
                    .setExchangeId(301L)
                    .setOrderId((System.currentTimeMillis() - beginId) * 10000 + rand)
                    .setClientOrderId("" + (System.currentTimeMillis() - beginId) + rand)
                    .build();
        }

        private void metricsRecode(long startTime, String method, String result) {
            long cost = System.currentTimeMillis() - startTime;
            requestLatency.labels(symbol, method, result).observe(cost);
        }
    }
}
