package io.bhex.broker.stress.controller;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bhex.base.account.*;
import io.bhex.base.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
@RequestMapping("/internal/shard_perf3")
public class ShardPerf3Controller {

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
                                 @RequestParam(defaultValue = "9001") Long orgId,
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
            PerfTask task = new PerfTask(grpcServer, grpcPort, orgId, count * nTradeThreads / nThreads / nThreads, startAid + aidCount * i, aidCount, symbol, basePrice);
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

    static class PerfTask implements Runnable {
        //成交订单占比
        int tradeCount;
        int startAid;
        int count;
        Long orgId;
        String symbol;
        BigDecimal basePrice;
        ManagedChannel tradeChannel;
        String grpcServer;
        Integer grpcPort;

        long startTime = System.currentTimeMillis();

        long beginId = 1615252523000L;

        BigDecimal priceDiff;

        ManagedChannelBuilder<?> channelBuilder;

        PerfTask(String grpcServer, int grpcPort, Long orgId, int tradeCount, int startAid, int count, String symbol, BigDecimal basePrice) {
            this.grpcServer = grpcServer;
            this.grpcPort = grpcPort;
            this.basePrice = basePrice;
            this.startAid = startAid;
            this.symbol = symbol;
            this.tradeCount = tradeCount;
            //必须是偶数
            if (count % 2 == 1) {
                count--;
            }
            if (tradeCount % 2 == 1) {
                tradeCount--;
            }
            this.count = count;
            this.orgId = orgId;

            this.priceDiff = basePrice.divide(new BigDecimal("2").multiply(new BigDecimal(count)), RoundingMode.DOWN);

            channelBuilder = ManagedChannelBuilder
                    .forAddress(grpcServer, grpcPort)
                    .usePlaintext();
            tradeChannel = channelBuilder.build();
            log.info("start task, startAid {},count {}, tradeCount {}, symbol {}", startAid, count, tradeCount, symbol);
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
                    if(tradeChannel != null) {
                        tradeChannel.shutdown();
                    }
                    tradeChannel = channelBuilder.build();
                    orderServiceBlockingStub = OrderServiceGrpc.newBlockingStub(tradeChannel);

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
                    //等待10毫秒秒
                    Thread.sleep(100);
                    cancelOrders();
                    //等待10毫秒秒
                    Thread.sleep(10);
                } catch (Exception e) {
                    log.error("exception: {}", e.getMessage(), e);
                }
            }
        }

        void dealOrder(Long aid, OrderSideEnum side, boolean isTrade) {
            NewOrderRequest newOrderRequest1 = buildNewOrderRequest(aid, side, isTrade);
            try {
                startTime = System.currentTimeMillis();
                NewOrderReply resp1 = orderServiceBlockingStub.newOrder(newOrderRequest1);
                if (!isTrade) {
                    orders.add(resp1.getOrder());
                }
                OrderStatusEnum orderStatus = resp1.getStatus();
                if (resp1.getTradeResponse().getFinishedCount() > 0) {
                    orderStatus = resp1.getTradeResponse().getFinished(0).getOrderStatus();
                }
//                log.info("resp: {},{}", orderStatus, resp1.getCode());
                metricsRecode(startTime, "pushOrder", "" + orderStatus);
            } catch (Exception e) {
                log.warn("pushOrder onError: {}", e.getMessage());
                metricsRecode(startTime, "pushOrder", e.getMessage());
            }
        }

        private void cancelOrders() {
            //一个个去撤单
            for (Order order : orders) {
                try {
                    startTime = System.currentTimeMillis();
                    CancelOrderReply reply1 = orderServiceBlockingStub.cancelOrder(CancelOrderRequest.newBuilder()
                            .setBaseRequest(BaseRequest.newBuilder().setOrganizationId(orgId).setBrokerUserId(order.getAccountId() + 100000).build())
                            .setAccountId(order.getAccountId())
                            .setOrderId(order.getOrderId())
                            .build());
                    metricsRecode(startTime, "cancelOrder", "cancel-" + reply1.getCode());
                } catch (Exception e) {
                    log.warn("cancelOrder onError: {}", e.getMessage(), e);
                    metricsRecode(startTime, "cancelOrder", e.getMessage());
                }
            }
        }

        private NewOrderRequest buildNewOrderRequest(Long accountId,
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
//            log.info("Aid {}, price: {}, side: {}, isTrade:{}", accountId, price, side, isTrade);
            BigDecimal qty = new BigDecimal("0.01");
            return NewOrderRequest.newBuilder()
                    .setAccountId(accountId)
                    .setQuantity(DecimalUtil.fromBigDecimal(qty))
                    .setPrice(DecimalUtil.fromBigDecimal(price))
                    .setAmount(DecimalUtil.fromBigDecimal(qty.multiply(price)))
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
