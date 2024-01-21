/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.controller.account
 *@Date 2018/6/10
 *@Author peiwei.ren@bhex.io
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.stress.controller;

import io.bhex.broker.stress.reactor.OrderReactor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * curl "http://127.0.0.1:7122/openapi/stress/order?duration=10000&threads=10&host=100.120.7.77:7120"
 */
@Slf4j
//@RestController
//@RequestMapping(value = "/openapi", produces = {"text/plain; charset=UTF-8"})
public class OrderStressController {

    @Resource
    private OrderReactor orderReactor;

    /**
     * @param duration          下单持续的时间，单位为秒
     * @param threads           下单或撤单的线程数量，即并发数量
     * @param host              传入的host， 即指该stress是调用哪个主机上的服务
     * @param report            是否将运行过程的时间信息统计上报到prometheus中，默认为false. 已经废弃，一直不上报
     * @param shouldMatch       是否下的单能够进行匹配与撮合，而不是仅仅挂着，默认进行能撮合的下单，即默认为true
     * @param doCancelSameTime  是否同时下撤消单，该开头会影响threads的数量，如果该值为true，则实际执行的thread的数量为
     *                          threads*2；默认不同时撤单，即该值默认为false
     * @param hostHeader        即调用被测试的服务时，传入的host header，默认为'www.bhex.jp'，这是一个专用于测试的域名
     * @param pricePrecision    生产订单时的价格的精度， 默认为4位.
     * @param quantityPrecision 生产订单时的数量精度，默认为4位
     * @param testTargetType        进行测试时的测试目标， 默认为1， 即broker-api， 相关值对应如下:
     *                          0:openapi, 1:broker-api, 2:broker-server, 3:bh-server, 4:shard-server, 5:match
     *                          6:option-api
     * @param symbol            进行测试时下单所选择的币对，默认为TETHTUSDT
     * @param exchangeId        进行测试时下单所选择的交易所Id， 默认为302
     * @param brokerId          进行测试时下单所选择的券商Id， 默认为6001
     *
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/stress/order")
    public String stressHttp(@RequestParam int duration,
                             @RequestParam int threads,
                             @RequestParam String host,
                             @RequestParam(required = false, defaultValue = "false") boolean report,
                             @RequestParam(required = false, defaultValue = "true") boolean shouldMatch,
                             @RequestParam(required = false, defaultValue = "false") boolean doCancelSameTime,
                             @RequestParam(required = false, defaultValue = "www.bhex.jp") String hostHeader,
                             @RequestParam(required = false, defaultValue = "4") long pricePrecision,
                             @RequestParam(required = false, defaultValue = "4") long quantityPrecision,
                             @RequestParam(required = false, defaultValue = "1") int testTargetType,
                             @RequestParam(required = false, defaultValue = "TETHTUSDT") String symbol,
                             @RequestParam(required = false, defaultValue = "302") long exchangeId,
                             @RequestParam(required = false, defaultValue = "6001") long brokerId

    ) throws Exception {
        log.info("## start stressHttp");
        String result = orderReactor.stressHttp(duration, threads, host,
                report, shouldMatch,
                doCancelSameTime, hostHeader,
                pricePrecision, quantityPrecision, testTargetType, symbol, exchangeId, brokerId);
        log.info("## end stressHttp");
        return result;
    }

}
