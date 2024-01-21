/*
 ************************************
 * @项目名称: openapi
 * @文件名称: HttpClient
 * @Date 2018/08/02
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.protocol.http;

import io.bhex.broker.stress.config.OrderConfig;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Param;
import org.asynchttpclient.Response;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

@Slf4j
public class HttpClient {

    public static Future<Response> get(String url, String hostHeader) {
        return get(url, hostHeader, 0);
    }

    public static Future<Response> get(String url, String hostHeader, long userId){
        BoundRequestBuilder builder = OrderConfig.client.prepareGet(url);
        builder = buildRequest(builder, hostHeader, userId);
        return builder.execute();
    }

    public static Future<Response> post(String url, List<Param> params, String hostHeader) {
        return post(url, params, hostHeader, 0);
    }

    public static Future<Response> post(String url, List<Param> params, String hostHeader, long userId) {
        BoundRequestBuilder builder = OrderConfig.client.preparePost(url).setFormParams(params);
        builder = buildRequest(builder, hostHeader, userId);
        return builder.execute();
    }

    public static Future<Response> delete(String url, List<Param> params, String hostHeader) {
        return  delete(url, params, hostHeader, 0);
    }

    public static Future<Response> delete(String url, List<Param> params, String hostHeader, long userId) {
        BoundRequestBuilder builder = OrderConfig.client.prepareDelete(url).setFormParams(params);
        builder = buildRequest(builder, hostHeader, userId);
        return builder.execute();
    }

    private static BoundRequestBuilder buildRequest(BoundRequestBuilder builder, String hostHeader, long userId){
        return builder
                .setCookies(Collections.singletonList(userId > 0 ? OrderConfig.ck(userId) : OrderConfig.c))
                .setHeader("Host", hostHeader)
                .setHeader("Origin", "http://" + hostHeader)
                .setHeader("User-Agent",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) " +
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36");
    }

}
