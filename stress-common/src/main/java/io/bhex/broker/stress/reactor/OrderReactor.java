/*
 ************************************
 * @项目名称: openapi
 * @文件名称: OrderReactor
 * @Date 2018/08/02
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.reactor;

import com.google.common.base.Throwables;
import io.bhex.base.account.NewOrderReply;
import io.bhex.base.account.NewOrderRequest;
import io.bhex.base.account.OrderServiceGrpc;
import io.bhex.base.grpc.client.channel.IGrpcClientPool;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.proto.OrderSideEnum;
import io.bhex.broker.stress.config.OrderConfig;
import io.bhex.broker.stress.executor.ThreadExecutor;
import io.bhex.broker.stress.histogram.HistogramStats;
import io.bhex.broker.stress.protocol.http.HttpClient;
import io.bhex.broker.stress.service.order.UserService;
import io.bhex.broker.stress.util.JsonUtil;
import io.grpc.ManagedChannel;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.apache.commons.collections4.MapUtils;
import org.asynchttpclient.Param;
import org.asynchttpclient.Response;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class OrderReactor extends HistogramStats {


    @Resource
    private IGrpcClientPool pool;
    private static final int DEFAULT_EXCHANGE_ID = 302;
    private static final int DEFAULT_BROKER_ID = 6001;
    private static final String DEFAULT_SYMBOL = "TETHTUSDT";
    private static final String SIDE_BUY = "buy";
    private static final String SIDE_SELL = "sell";
    private static final String ORDER_TYPE_LIMIT = "limit";

    private final static int USER_ID_START = 30001;
    private final static int USER_ID_END = 50000;

    private final static double PRICE_L1 = 100.0D;
    private final static double PRICE_L2 = 102.0D;
    private final static double PRICE_L3 = 104.0D;
    private final static double PRICE_L4 = 106.0D;

    private final static double QUANTITY_START = 0.01D;
    private final static double QUANTITY_END = 0.02D;

    private final static int HIGH_COST_WATER_MARK = 80;

    private final static AtomicInteger currentUserId = new AtomicInteger(USER_ID_START);

    @Resource
    private UserService userService;
    private static AtomicLong clientIDGen = new AtomicLong(System.currentTimeMillis());
    private ThreadPoolTaskExecutor executor;
    private Random random = new Random();

    @PostConstruct
    public void init() {

    }

    /**
     * @param duration          下单持续的时间，单位为秒
     * @param threads           下单或撤单的线程数量，即并发数量
     * @param host              传入的host， 即指该stress是调用哪个主机上的服务
     * @param report            是否将运行过程的时间信息统计上报到prometheus中，默认为false. 已经废弃，一直不上报
     * @param shouldMatch       是否下的单能够进行匹配与撮合，而不是仅仅挂着，默认进行能撮合的下单，即默认为true
     * @param doCancelSameTime  是否同时下撤消单，该开关会影响threads的数量，如果该值为true，则实际执行的thread的数量为
     *                          threads*2；默认不同时撤单，即该值默认为false
     * @param hostHeader        即调用被测试的服务时，传入的host header，默认为'www.bhex.jp'，这是一个专用于测试的域名
     * @param pricePrecision    生产订单时的价格的精度， 默认为4位.
     * @param quantityPrecision 生产订单时的数量精度，默认为4位
     * @param testTargetType    进行测试时的测试目标， 默认为1， 即broker-api， 相关值对应如下:
     *                          0:openapi, 1:broker-api, 2:broker-server, 3:bh-server, 4:shard-server, 5:match
     *                          6:option-api
     * @param symbol            进行测试时下单所选择的币对，默认为TETHTUSDT
     * @param exchangeId        进行测试时下单所选择的交易所Id， 默认为302
     * @param brokerId          进行测试时下单所选择的券商Id， 默认为6001
     * @return
     * @throws Exception
     */
    public String stressHttp(int duration, int threads, String host, boolean report,
                             boolean shouldMatch, boolean doCancelSameTime, String hostHeader,
                             long pricePrecision, long quantityPrecision, int testTargetType, String symbol, long exchangeId, long brokerId) throws Exception {
        Histogram httpHistogram = new Histogram(HISTOGRAM_MAX_VALUE, HISTOGRAM_PRECISION);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;

        //if docancelsame time is true,  one for orderRequest2HttpBroker, one for cancel
        int threadCount = doCancelSameTime ? threads * 2 : threads;

        executor = ThreadExecutor.executor(threadCount);
        executor.setWaitForTasksToCompleteOnShutdown(true);

        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                for (; ; ) {
                    long now = System.currentTimeMillis();
                    if (now > endTime) {
                        break;
                    }
                    try {
                        OrderMemo orderMemo = buildOrderMemo(shouldMatch, pricePrecision,
                                quantityPrecision, symbol,
                                exchangeId, brokerId);

                        orderQps(
                                orderMemo,
                                httpHistogram, host,
                                report,
                                startTime,
                                doCancelSameTime,
                                hostHeader,
                                testTargetType);
                    } catch (Exception e) {
                        log.error("orderQps error", e);
                    }
                }

                latch.countDown();
            });
        }

        //Thread.sleep(duration);
        try {
            latch.await();
            executor.shutdown();
        } catch (Exception e) {
            log.error(Throwables.getStackTraceAsString(e));

        }

        log.info("executor = {}", executor.getThreadPoolExecutor().toString());
        long elapsedTime = System.currentTimeMillis() - startTime;
        return printStats(httpHistogram, elapsedTime, executor.getCorePoolSize());
    }

    private OrderMemo buildOrderMemo(boolean shouldMath, long pricePrecision,
                                     long quantityPrecision, String symbol,
                                     long exchangeId, long brokerId) {
        double quality = QUANTITY_START + (QUANTITY_END - QUANTITY_START) * random.nextDouble();
        boolean isBuy = random.nextBoolean();
        double price = 0.0D;
        if (shouldMath) {
            price = PRICE_L2 + (PRICE_L3 - PRICE_L2) * random.nextDouble();
        } else {
            if (isBuy) {
                price = PRICE_L1 + (PRICE_L2 - PRICE_L1) * random.nextDouble();
            } else {
                price = PRICE_L3 + (PRICE_L4 - PRICE_L3) * random.nextDouble();
            }
        }

        OrderMemo.OrderMemoBuilder builder = OrderMemo.builder()
                .side(isBuy ? SIDE_BUY : SIDE_SELL)
                .symbolId(symbol)
                .clientOrderId(String.valueOf(clientIDGen.getAndIncrement()))
                .exchangeId(exchangeId)
                .type(ORDER_TYPE_LIMIT)
                .quantity(String.format("%." + quantityPrecision + "f", quality))
                .price(String.format("%." + pricePrecision + "f", price))
                .userId(nextUserId())
                .brokerId(brokerId);

        return builder.build();
    }


    public synchronized int nextUserId() {
        if (currentUserId.compareAndSet(USER_ID_END, USER_ID_START)) {
            return currentUserId.get();
        } else {
            return currentUserId.getAndIncrement();
        }
    }

    public static void main(String... args) {
        OrderReactor reactor = new OrderReactor();
        OrderMemo memo = reactor.buildOrderMemo(true, 2, 4, DEFAULT_SYMBOL, DEFAULT_EXCHANGE_ID, DEFAULT_BROKER_ID);
        System.out.println(memo);


    }

    private void orderQps(OrderMemo memo, Histogram httpHistogram, String host,
                          boolean report, long startTime,
                          boolean doCancelSameTime, String hostHeader,
                          int testTargetType) {
        String newOrderResponse = null;
        final long startNewOrder = System.currentTimeMillis();

        switch (testTargetType) {
            case 0: //invoke open-api
            case 1: //invoke broker-api
            case 6: //invoke option api
                newOrderResponse = orderRequest2HttpBroker(memo, host, hostHeader, testTargetType);
                break;
            case 2:
                //invoke broker-server
                break;
            case 3:
                //invoke bh-server
                newOrderResponse = orderRequest2BHServer(memo, host);
                break;
            case 4:
                //invoke shard-server
                break;
            case 5:
                break;
            default:
                break;
        }


        if (Optional.ofNullable(newOrderResponse).isPresent()) {
            final long endNewOrder = System.currentTimeMillis();
            final long newOrderCost = endNewOrder - startNewOrder;
            httpHistogram.recordValue(newOrderCost);
            if (newOrderCost > HIGH_COST_WATER_MARK) {
                log.info("orderRequest2HttpBroker high cost |{}|{}|{}", memo.userId, newOrderResponse, newOrderCost);
            }


            if (doCancelSameTime) {
                final long startCancelOrder = System.currentTimeMillis();
                String cancelOrderId = cancelOrder2HttpBroker(memo.clientOrderId, memo.userId, newOrderResponse, host, hostHeader, testTargetType);

                if (Optional.ofNullable(cancelOrderId).isPresent()) {
                    final long endCancelOrder = System.currentTimeMillis();
                    long cancelCost = endCancelOrder - startCancelOrder;
                    httpHistogram.recordValue(cancelCost);

                    if (cancelCost > HIGH_COST_WATER_MARK) {
                        log.info("CancelOrder high cost |{}|{}|{}", memo.userId, newOrderResponse, cancelCost);
                    }

                }

            }

            //每10000个订单打印一次统计信息
            if (memo.clientOrderId.endsWith("0000")) {
                printStats(httpHistogram, System.currentTimeMillis() - startTime, executor.getActiveCount());
            }

        }
    }

    private String orderRequest2BHServer(OrderMemo memo, String host) {
        String result = null;

        NewOrderRequest.Builder builder = NewOrderRequest.newBuilder()
                .setAccountId(Long.valueOf("1" + memo.getUserId()))
                .setClientOrderId(memo.getClientOrderId())
                .setSide(OrderSideEnum.valueOf(memo.getSide().toUpperCase()))
                .setExchangeId(memo.getExchangeId())
                .setSymbolId(memo.getSymbolId())
                .setOrgId(memo.getBrokerId())
                .setPrice(DecimalUtil.fromBigDecimal(new BigDecimal(memo.getPrice())))
                .setQuantity(DecimalUtil.fromBigDecimal(new BigDecimal(memo.getQuantity())));

        try {
            ManagedChannel channel = getChannel(host);
            OrderServiceGrpc.OrderServiceBlockingStub stub =
                    OrderServiceGrpc.newBlockingStub(channel);
            //.withDeadlineAfter(10000, TimeUnit.MILLISECONDS);
            NewOrderReply reply = stub.newOrder(builder.build());
            result = String.valueOf(reply.getOrderId());
        } catch (Exception e) {
            log.error("New order to bh-server occured error :{}", Throwables.getStackTraceAsString(e));
        }

        return result;
    }

    private ManagedChannel getChannel(String host) throws Exception {
        ManagedChannel channel = pool.borrowChannel(host);
        if (channel == null) {
            String[] hostPort = host.split(":");
            String hostName = hostPort[0];
            int port = Integer.valueOf(hostPort[1]).intValue();

            pool.setShortcut(host, hostName, port);
            channel = pool.borrowChannel(host);
        }

        if (channel == null) throw new Exception(String.format("Can't got a channel for host %s ", host));

        return channel;
    }

    private String orderRequest2HttpBroker(OrderMemo memo, String host, String hostHeader, int targetType) {
        Response response;
        try {
            List<Param> params = new ArrayList<>();
            params.add(new Param("exchange_id", String.valueOf(memo.exchangeId)));
            params.add(new Param("client_order_id", memo.clientOrderId));
            params.add(new Param("user_id", String.valueOf(memo.userId)));
            params.add(new Param("symbol_id", memo.symbolId));
            params.add(new Param("side", memo.side));
            params.add(new Param("type", memo.type));
            params.add(new Param("price", memo.price));
            params.add(new Param("quantity", memo.quantity));

            String url = getRequestUrl(targetType, true, host);
            Future<Response> future = HttpClient.post(url, params, hostHeader);

            response = future.get();
        } catch (Exception e) {
            log.error("createOrder error: {}", Throwables.getStackTraceAsString(e));
            return null;
        }

        if (response.getStatusCode() != 200) {
            log.info("createOrder failed|{}|{}", memo.userId, response.getResponseBody());
            return null;
        }
        return MapUtils.getString(JsonUtil.toMap(response.getResponseBody()), "orderId");
    }

    private String getRequestUrl(int targetType, boolean isNewOrderRequest, String host) {
        String url = null;
        switch (targetType) {
            case 0:
                if (isNewOrderRequest) {
                    url = String.format(OrderConfig.OPEN_API_CREATE_ORDER_URL, host);
                } else {
                    url = String.format(OrderConfig.OPEN_API_CANCEL_ORDER_URL, host);
                }
                break;

            case 1:
                if (isNewOrderRequest) {
                    url = String.format(OrderConfig.BROKER_API_CREATE_ORDER_URL, host);
                } else {
                    url = String.format(OrderConfig.BROKER_API_CANCEL_ORDER_URL, host);
                }
                break;

            case 6:
                if (isNewOrderRequest) {
                    url = String.format(OrderConfig.OPTION_API_CREATE_ORDER_URL, host);
                } else {
                    url = String.format(OrderConfig.OPTION_API_CANCEL_ORDER_URL, host);
                }
                break;
            default:
                break;
        }

        return url;
    }

    private String cancelOrder2HttpBroker(String clientOrderId, Long userId, String orderId,
                                          String host, String hostHeader, int targetType) {
        Response response;
        try {
            List<Param> params = new ArrayList<>();
            params.add(new Param("user_id", String.valueOf(userId)));
            params.add(new Param("client_order_id", clientOrderId));
            params.add(new Param("order_id", orderId));


            String url = getRequestUrl(targetType, false, host);
            Future<Response> future = null;

            if (targetType == 0) {
                future = HttpClient.delete(url, params, hostHeader);
            } else {
                future = HttpClient.post(url, params, hostHeader);
            }

            response = future.get();
        } catch (Exception e) {
            log.error("cancel order error:{}", Throwables.getStackTraceAsString(e));
            return null;
        }

        if (response.getStatusCode() != 200) {
            log.info("cancelOrder2HttpBroker failed|{}|{}", userId, response.getResponseBody());
            return null;
        }

        return MapUtils.getString(JsonUtil.toMap(response.getResponseBody()), "orderId");
    }

}

@Data
@Builder
class OrderMemo {
    long userId;
    long exchangeId;
    String symbolId;
    String clientOrderId;
    String side;
    String type;
    String quantity;
    String price;
    long brokerId;
}


