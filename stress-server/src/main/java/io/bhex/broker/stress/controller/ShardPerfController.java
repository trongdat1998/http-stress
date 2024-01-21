package io.bhex.broker.stress.controller;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bhex.base.account.*;
import io.bhex.base.metrics.PrometheusMetricsCollector;
import io.bhex.base.proto.BaseRequest;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.base.proto.OrderTypeEnum;
import io.bhex.broker.stress.util.JsonUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
@RestController
@RequestMapping("/internal/shard_perf")
public class ShardPerfController {

    static int exchangeId = 301;
    static int brokerId = 6002;

    static ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("perf-%d").build();
    static ThreadPoolExecutor executor;

    static int logPerRequest = 2000;

    static boolean stop = false;

    public static final Histogram requestLatency = Histogram.build()
            .name("bh_requests_latency_milliseconds")
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
                                 @RequestParam(defaultValue = "1000") Integer basePrice,
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
            PerfTask task = new PerfTask(grpcServer, grpcPort, 6001l, i < nTradeThreads, startAid + aidCount * i, aidCount, symbol, basePrice);
            executor.submit(task);
        }

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

    AccountServiceGrpc.AccountServiceBlockingStub accountServiceStub;

    @RequestMapping("/create_accounts")
    public String createAccounts(@RequestParam(defaultValue = "127.0.0.1") String grpcServer,
                                 @RequestParam(defaultValue = "7011") Integer grpcPort,
                                 @RequestParam(defaultValue = "6001") Long orgId,
                                 @RequestParam(defaultValue = "1") Integer count) {
        if (accountServiceStub == null) {
            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                    .forAddress(grpcServer, grpcPort)
                    .usePlaintext();
            ManagedChannel channel = channelBuilder.build();
            accountServiceStub = AccountServiceGrpc.newBlockingStub(channel);
        }
        List<String> accounts = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            SimpleCreateAccountReply resp = accountServiceStub.simpleCreateAccount(SimpleCreateAccountRequest.newBuilder()
                    .setOrgId(orgId)
                    .setUserId("0")
                    .build());
            accounts.add(resp.getBhUserId() + "-" + resp.getAccountId());
            log.info("create simple account: {},{}", resp.getBhUserId(), resp.getAccountId());
        }
        return JsonUtil.toJson(accounts);
    }

    static class PerfTask implements Runnable {
        boolean trade;
        int startAid;
        int count;
        Long orgId;
        String symbol;
        double basePrice;
        ManagedChannel tradeChannel;

        long startTime = System.currentTimeMillis();

        long beginId = 1615252523000L;

        double priceDiff;

        PerfTask(String grpcServer, int grpcPort, Long orgId, boolean trade, int startAid, int count, String symbol, double basePrice) {
            this.basePrice = basePrice;
            this.startAid = startAid;
            this.symbol = symbol;
            this.trade = trade;
            this.count = count;
            this.orgId = orgId;

            this.priceDiff = basePrice / 2 / count;

            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                    .forAddress(grpcServer, grpcPort)
                    .usePlaintext();
            tradeChannel = channelBuilder.build();
            log.info("start task, startAid {},count {}, trade {}, symbol {}", startAid, count, trade, symbol);
        }

        @Override
        public void run() {
            long i = 0;
            //循环下单
            while (true) {
                try {
                    if (stop) {
                        break;
                    }
                    startTime = System.currentTimeMillis();
                    i += 2;
                    boolean reverse = (i % 4) % 2 == 0;
                    if (reverse) {
                        dealOrder(this.startAid + i % count, this.startAid + i % count + 1, i % logPerRequest == 0);
                    } else {
                        dealOrder(this.startAid + i % count + 1, this.startAid + i % count, i % logPerRequest == 0);
                    }
                    //等待10秒
                    Thread.sleep(10);
                } catch (Exception e) {
                    log.error("exception: {}", e.getMessage(), e);
                }
            }
        }

        void dealOrder(Long aid1, Long aid2, boolean doLog) {
            OrderServiceGrpc.OrderServiceBlockingStub orderServiceBlockingStub = OrderServiceGrpc.newBlockingStub(tradeChannel);
            NewOrderRequest newOrderRequest1 = buildNewOrderRequest(aid1, OrderSideEnum.BUY);
            Long orderId1 = null;
            Long orderId2 = null;
            try {
                startTime = System.currentTimeMillis();
                NewOrderReply resp1 = orderServiceBlockingStub.newOrder(newOrderRequest1);
                if (doLog) {
                    log.info("pushOrder: order id {}", resp1.getOrderId());
                }
                orderId1 = resp1.getOrderId();
                metricsRecode(startTime, "pushOrder", "" + resp1.getStatus());
            } catch (Exception e) {
                log.warn("pushOrder1 onError: {}", e.getMessage());
                metricsRecode(startTime, "pushOrder", "fail");
            }

            NewOrderRequest newOrderRequest2 = buildNewOrderRequest(aid2, OrderSideEnum.SELL);
            try {
                startTime = System.currentTimeMillis();
                NewOrderReply resp2 = orderServiceBlockingStub.newOrder(newOrderRequest2);
                if (doLog) {
                    log.info("pushOrder: order id {}", resp2.getOrderId());
                }
                orderId2 = resp2.getOrderId();
                metricsRecode(startTime, "pushOrder", "" + resp2.getStatus());
            } catch (Exception e) {
                log.warn("pushOrder2 onError: {}", e.getMessage());
                metricsRecode(startTime, "pushOrder", "fail");
            }
            if (!trade) {
                try {
                    startTime = System.currentTimeMillis();
                    CancelOrderReply reply1 = orderServiceBlockingStub.cancelOrder(CancelOrderRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(aid1 + 100000).build())
                            .setAccountId(aid1)
                            .setOrderId(orderId1)
                            .build());
                    metricsRecode(startTime, "cancelOrder", "cancel-" + reply1.getStatus());
                } catch (Exception e) {
                    log.warn("cancelOrder1 onError: {}", e.getMessage(), e);
                    metricsRecode(startTime, "cancelOrder", "fail");
                }
                try {
                    startTime = System.currentTimeMillis();
                    CancelOrderReply reply2 = orderServiceBlockingStub.cancelOrder(CancelOrderRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(aid2 + 100000).build())
                            .setAccountId(aid2)
                            .setOrderId(orderId2)
                            .build());
                    metricsRecode(startTime, "cancelOrder", "cancel-" + reply2.getStatus());
                } catch (Exception e) {
                    log.warn("cancelOrder2 onError: {}", e.getMessage(), e);
                    metricsRecode(startTime, "cancelOrder", "fail");
                }
            } else {
//                try {
//                    Thread.sleep(100);
//                } catch (Exception e) {
//                }
//                try {
//                    startTime = System.currentTimeMillis();
//                    GetOrderReply reply1 = orderServiceBlockingStub.getOrder(GetOrderRequest.newBuilder()
//                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(aid1 + 100000).build())
//                            .setAccountId(aid1)
//                            .setOrderId(orderId1)
//                            .build());
//                    if (reply1.getOrder().getStatus() == OrderStatusEnum.FILLED) {
//                        metricsRecode(startTime, "getOrder", "succ");
//                    } else {
//                        if (doLog) {
//                            log.warn("getOrder1 onWarn: status={},client_order_id={}", reply1.getOrder().getStatus(), reply1.getOrder().getClientOrderId());
//                        }
//                        metricsRecode(startTime, "getOrder", "warn");
//                    }
//                } catch (Exception e) {
//                    log.warn("getOrder1 onError: {}", e.getMessage(), e);
//                    metricsRecode(startTime, "getOrder", "fail");
//                }
//                try {
//                    startTime = System.currentTimeMillis();
//                    GetOrderReply reply2 = orderServiceBlockingStub.getOrder(GetOrderRequest.newBuilder()
//                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(aid2 + 100000).build())
//                            .setAccountId(aid2)
//                            .setOrderId(orderId2)
//                            .build());
//                    if (reply2.getOrder().getStatus() == OrderStatusEnum.FILLED) {
//                        metricsRecode(startTime, "getOrder", "succ");
//                    } else {
//                        if (doLog) {
//                            log.warn("getOrder2 onWarn: status={},client_order_id={}", reply2.getOrder().getStatus(), reply2.getOrder().getClientOrderId());
//                        }
//                        metricsRecode(startTime, "getOrder", "warn");
//                    }
//                } catch (Exception e) {
//                    log.warn("getOrder2 onError: {}", e.getMessage(), e);
//                    metricsRecode(startTime, "getOrder", "fail");
//                }
            }
        }

        private NewOrderRequest buildNewOrderRequest(Long accountId,
                                                     OrderSideEnum side) {

            double step = priceDiff * (accountId % count);

            //默认买卖同价，确保成交
            double price;
            if (!trade) {
                if (side == OrderSideEnum.BUY) {
                    price = basePrice - step;
                } else {
                    price = basePrice + step;//逐步升高
                }
            } else {
                if (side == OrderSideEnum.BUY) {
                    price = basePrice + step / 2;
                } else {
                    price = basePrice - step / 2;
                }
            }
            double qty = 0.01;
            return NewOrderRequest.newBuilder()
                    .setAccountId(accountId)
                    .setQuantity(DecimalUtil.fromDouble(qty))
                    .setPrice(DecimalUtil.fromDouble(price))
                    .setSymbolId(symbol)
                    .setOrderType(OrderTypeEnum.LIMIT)
                    .setOrgId(orgId)
                    .setSide(side)
                    .setExchangeId(301L)
                    .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setAccountId(accountId).setBrokerUserId(accountId + 100000).build())
                    .setClientOrderId("perf_" + accountId + "_" + (System.currentTimeMillis() - beginId) + RandomUtils.nextInt(1000, 9999))
                    .build();
        }

        private void metricsRecode(long startTime, String method, String result) {
            long cost = System.currentTimeMillis() - startTime;
            requestLatency.labels(symbol, method, result).observe(cost);
        }
    }
}
