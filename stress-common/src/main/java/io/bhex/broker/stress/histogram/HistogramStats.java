/*
 ************************************
 * @项目名称: openapi
 * @文件名称: HistogramUtil
 * @Date 2018/08/15
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.histogram;

import io.bhex.broker.stress.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;

import java.util.Date;

@Slf4j
public class HistogramStats {

    // The histogram can record values between 1 microsecond and 1 min.
    protected final long HISTOGRAM_MAX_VALUE = 60000000L;

    // Value quantization will be no more than 1%. See the README of HdrHistogram for more details.
    protected final int HISTOGRAM_PRECISION = 2;

    protected String printStats(Histogram histogram, long elapsedTime, int concurrently) {
        long latency50 = histogram.getValueAtPercentile(50);
        long latency70 = histogram.getValueAtPercentile(70);
        long latency80 = histogram.getValueAtPercentile(80);
        long latency90 = histogram.getValueAtPercentile(90);
        long latency95 = histogram.getValueAtPercentile(95);
        long latency99 = histogram.getValueAtPercentile(99);
        long latencyMax = histogram.getValueAtPercentile(100);

        float queriesPerSecond = 0;
        if(elapsedTime > 0){
            queriesPerSecond = histogram.getTotalCount() / ((float)elapsedTime) * 1000;
        }

        StringBuilder values = new StringBuilder("\n")
                .append("================================").append('\n')
                .append("Statistic Time:             ").append(new Date()).append("\n")
                .append("Concurrent:                 ").append(concurrently).append("\n")
                .append("TotalCount:                 ").append(histogram.getTotalCount()).append("\n")
                .append("ElpasedTime (in sec):       ").append((float)elapsedTime / 1000).append("\n")
                .append("50%ile Latency (in ms):     ").append(latency50).append('\n')
                .append("70%ile Latency (in ms):     ").append(latency70).append('\n')
                .append("80%ile Latency (in ms):     ").append(latency80).append('\n')
                .append("90%ile Latency (in ms):     ").append(latency90).append('\n')
                .append("95%ile Latency (in ms):     ").append(latency95).append('\n')
                .append("99%ile Latency (in ms):     ").append(latency99).append('\n')
                .append("Maximum Latency (in ms):    ").append(latencyMax).append('\n')
                .append("QPS:                        ").append(queriesPerSecond).append('\n')
                .append("================================\n");
        log.info("result = {}", values);
        return values.toString();
    }


    public static void main(String...args){
       HistogramStats stats = new HistogramStats();
       Histogram histogram = new Histogram(stats.HISTOGRAM_MAX_VALUE, stats.HISTOGRAM_PRECISION);

       histogram.recordValue(10);
       histogram.recordValue(20);
       histogram.recordValue(30);
       stats.printStats(histogram, 2, 4);
    }
}
