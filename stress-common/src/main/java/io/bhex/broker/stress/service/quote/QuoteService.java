/*
 ************************************
 * @项目名称: openapi
 * @文件名称: QuoteService
 * @Date 2018/08/07
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.service.quote;

import io.bhex.broker.stress.config.QuoteConfig;
import io.bhex.broker.stress.protocol.http.HttpClient;
import org.asynchttpclient.Response;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
public class QuoteService {

    public Future<Response> trade(String host) {
        return HttpClient.get(String.format(QuoteConfig.tradeURL, host), "www.bhex.jp");
    }


}
