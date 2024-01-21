/*
 ************************************
 * @项目名称: openapi
 * @文件名称: QuoteReactor
 * @Date 2018/08/02
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.reactor;

import io.bhex.broker.stress.config.QuoteConfig;
import io.bhex.broker.stress.histogram.HistogramStats;
import io.bhex.broker.stress.protocol.websocket.WebSocketClient;
import io.bhex.broker.stress.executor.ThreadExecutor;
import io.bhex.broker.stress.service.quote.QuoteService;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.asynchttpclient.Response;
import org.asynchttpclient.ws.WebSocket;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

@Slf4j
@Component
public class QuoteReactor extends HistogramStats {

    private static WebSocketClient socketClient = new WebSocketClient();

    @Resource
    private QuoteService quoteService;

    public void stressHttp(int duration, int threads, String host) throws Exception {
        Histogram httpHistogram = new Histogram(HISTOGRAM_MAX_VALUE, HISTOGRAM_PRECISION);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration;
        ThreadPoolTaskExecutor executor = ThreadExecutor.executor(threads);
        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                for (; ; ) {
                    long now = System.currentTimeMillis();
                    if (now > endTime) {
                        break;
                    }
                    try {
                        orderQps(httpHistogram, host);
                    } catch (Exception e) {
                        log.error("orderQps error", e);
                    }
                }
            });
        }

        Thread.sleep(duration);
        log.info("executor = {}", executor.getThreadPoolExecutor().toString());

        long elapsedTime = System.currentTimeMillis() - startTime;
        printStats(httpHistogram, elapsedTime, executor.getActiveCount());
    }

    private void orderQps(Histogram httpHistogram, String host) throws Exception {
        long start = System.currentTimeMillis();
        Future<Response> future = quoteService.trade(host);

        int status = future.get().getStatusCode();
        if (status == 200) {
            long cost = System.currentTimeMillis() - start;
            httpHistogram.recordValue(cost);
//            log.info("quoteAccess|{}|{}", (System.currentTimeMillis() - start), status);
        }
    }

    public void stressWebSocket(int duration, int connects, String host) throws Exception {
        long startTime = System.currentTimeMillis();
        Histogram wssHistogram = new Histogram(HISTOGRAM_MAX_VALUE, HISTOGRAM_PRECISION);
        List<WebSocket> list = new ArrayList<>();
        for (int i = 0; i < connects; i++) {
            long start = System.currentTimeMillis();
            WebSocket ws = socketClient.get(String.format(QuoteConfig.wssURL, host));
            list.add(ws);
            log.info("ws = {}", ws);

            if (ws.isOpen()) {
                long cost = System.currentTimeMillis() - start;
                wssHistogram.recordValue(cost);
            }
        }

        Thread.sleep(duration);


        list.forEach(WebSocket::sendCloseFrame);
        long elapsedTime = System.currentTimeMillis() - startTime;
        printStats(wssHistogram, elapsedTime, connects);
    }
}
