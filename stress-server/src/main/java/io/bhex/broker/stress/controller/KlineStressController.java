/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.controller.account
 *@Date 2018/6/10
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.stress.controller;

import io.bhex.broker.stress.reactor.QuoteReactor;
import io.bhex.broker.stress.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * curl "http://127.0.0.1:7122/openapi/stress/http_quote?duration=10000&threads=10&host=10.103.3.16:7121"
 * curl "http://127.0.0.1:7122/openapi/stress/web_socket_quote?duration=10000&connects=10&host=10.103.3.16:7121"
 */
@Slf4j
//@RestController
//@RequestMapping(value = "/openapi", produces = {"text/plain; charset=UTF-8"})
public class KlineStressController {

    @Resource
    private QuoteReactor quoteReactor;

    @RequestMapping(value = "/stress/http_quote")
    public String stressHttp(@RequestParam int duration, @RequestParam int threads, @RequestParam String host) throws Exception {
        log.info("## start stressHttp");
        quoteReactor.stressHttp(duration, threads, host);
        log.info("## end stressHttp");
        return JsonUtil.defaultGson().toJson("OK");
    }

    @RequestMapping(value = "/stress/web_socket_quote")
    public String stressWebSocket(@RequestParam int duration, @RequestParam int connects, @RequestParam String host) throws Exception {
        log.info("start stressWebSocket");
        quoteReactor.stressWebSocket(duration, connects, host);
        log.info("end stressWebSocket");
        return JsonUtil.defaultGson().toJson("OK");
    }
}
