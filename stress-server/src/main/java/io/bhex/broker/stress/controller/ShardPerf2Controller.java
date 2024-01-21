package io.bhex.broker.stress.controller;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bhex.base.account.*;
import io.bhex.base.proto.*;
import io.bhex.broker.stress.util.JsonUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
@RequestMapping("/internal/shard_perf2")
public class ShardPerf2Controller {

    static ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("perf-%d").build();
    static ThreadPoolExecutor executor;

    static int logPerRequest = 2000;

    static boolean stop = false;

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

        List<Order> orders = new LinkedList<>();
        OrderServiceGrpc.OrderServiceBlockingStub orderServiceBlockingStub;

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
                    orderServiceBlockingStub = OrderServiceGrpc.newBlockingStub(tradeChannel);

                    long i = 0;
                    //先下单
                    for (; i < count; i += 2) {
                        if (reverse) {
                            dealOrder(this.startAid + i, this.startAid + i + 1);
                        } else {
                            dealOrder(this.startAid + i + 1, this.startAid + i);
                        }
                    }
                    cancelOrders();
                    //等待10秒
                    Thread.sleep(10);
                } catch (Exception e) {
                    log.error("exception: {}", e.getMessage(), e);
                }
            }
        }

        void dealOrder(Long aid1, Long aid2) {
            NewOrderRequest newOrderRequest1 = buildNewOrderRequest(aid1, OrderSideEnum.BUY);
            try {
                startTime = System.currentTimeMillis();
                NewOrderReply resp1 = orderServiceBlockingStub.newOrder(newOrderRequest1);
                orders.add(resp1.getOrder());
//                log.info("resp1:{}",resp1.getOrder().getStatus());
                metricsRecode(startTime, "pushOrder", "" + resp1.getStatus());
            } catch (Exception e) {
                log.warn("pushOrder1 onError: {}", e.getMessage());
                metricsRecode(startTime, "pushOrder", "fail");
            }

            NewOrderRequest newOrderRequest2 = buildNewOrderRequest(aid2, OrderSideEnum.SELL);
            try {
                startTime = System.currentTimeMillis();
                NewOrderReply resp2 = orderServiceBlockingStub.newOrder(newOrderRequest2);
                orders.add(resp2.getOrder());
//                log.info("resp1:{}",resp2.getOrder().getStatus());
                metricsRecode(startTime, "pushOrder", "" + resp2.getStatus());
            } catch (Exception e) {
                log.warn("pushOrder2 onError: {}", e.getMessage());
                metricsRecode(startTime, "pushOrder", "fail");
            }
        }

        private void cancelOrders() {
            if(!trade) {
                //一个个去撤单
                for (Order order : orders) {
                    try {
                        startTime = System.currentTimeMillis();
                        CancelOrderReply reply1 = orderServiceBlockingStub.cancelOrder(CancelOrderRequest.newBuilder()
                                .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(order.getAccountId() + 100000).build())
                                .setAccountId(order.getAccountId())
                                .setOrderId(order.getOrderId())
                                .build());
                        if (reply1.getCode() == ErrorCode.SUCCESS) {
                            metricsRecode(startTime, "cancelOrder", "cancel-" + reply1.getStatus());
                        } else {
                            metricsRecode(startTime, "cancelOrder", "cancel-" + reply1.getCode());
                        }
                    } catch (Exception e) {
                        log.warn("cancelOrder onError: {}", e.getMessage(), e);
                        metricsRecode(startTime, "cancelOrder", "fail");
                    }
                }
            }
        }

        private NewOrderRequest buildNewOrderRequest(Long accountId,
                                                     OrderSideEnum side) {

            double step = priceDiff * (accountId - startAid);

            //默认买卖同价，确保成交
            double price;
            if (!trade) {
                if (side == OrderSideEnum.BUY) {
                    price = basePrice - step;
                } else {
                    price = basePrice + step;//逐步升高
                }
            } else {
//                if (side == OrderSideEnum.BUY) {
//                    price = basePrice + priceDiff / 2;
//                } else {
//                    price = basePrice - priceDiff / 2;
//                }
                price = basePrice;
            }
//            log.info("Aid {}, price: {}, side: {}, isTrade:{}", accountId, price, side, trade);
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
            ShardPerfController.requestLatency.labels(symbol, method, result).observe(cost);
        }
    }
}
