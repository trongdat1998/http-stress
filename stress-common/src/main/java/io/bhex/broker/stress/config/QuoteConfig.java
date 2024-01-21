/*
 ************************************
 * @项目名称: openapi
 * @文件名称: QuoteConfif
 * @Date 2018/08/07
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.config;

import org.asynchttpclient.AsyncHttpClient;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class QuoteConfig {

    public static final AsyncHttpClient client = asyncHttpClient();

    public static String tradeURL = "http://%s/quote/trade?exchangeId=301&symbol=BCHBTC";

    public static String wssURL = "ws://%s/ws/301.BCHBTC@trade_20";

}

