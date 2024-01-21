/*
 ************************************
 * @项目名称: openapi
 * @文件名称: OrderConfig
 * @Date 2018/08/07
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.config;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import org.asynchttpclient.AsyncHttpClient;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class OrderConfig {

    public static final AsyncHttpClient client = asyncHttpClient();

    public static final String OPTION_API_CREATE_ORDER_URL = "http://%s/internal/option/order/create"; //post operation
    public static final String OPTION_API_CANCEL_ORDER_URL = "http://%s/internal/option/order/cancel"; //post operation
    public static final String OPTION_API_ANNALYZE_URL = "http://%s/s_api/analyze";

    public static final String BROKER_API_CREATE_ORDER_URL = "http://%s/internal/order/create"; //post operation
    public static final String BROKER_API_CANCEL_ORDER_URL = "http://%s/internal/order/cancel"; //post operation
    public static final String BROKER_API_ANNALYZE_URL = "http://%s/s_api/analyze";

    public static final String BH_SERVER_CREATE_ORDER_URL = "http://%s/internal/order/create"; //post operation
    public static final String BH_SERVER_CANCEL_ORDER_URL = "http://%s/internal/order/cancel"; //post operation
    public static final String BH_SERVER_ANALYXE_URL = "http://%s/s_api/analyze";

    public static final String OPEN_API_CREATE_ORDER_URL = "http://%s/openapi/order"; //post operation
    public static final String OPEN_API_CANCEL_ORDER_URL = "http://%s/openapi/order"; //delete operation
    public static final String OPEN_API_ANNALYZE_URL = "http://%s/s_api/analyze";

    public static Cookie c = new DefaultCookie("au_token", getToken(91859285801697280L));

    public static Cookie ck(long userId){
        return new DefaultCookie("au_token", getToken(userId));
    }

    private static String getToken(Long userId) {
        String randomKey = "abcdefg12345";
        return Jwts.builder()
                .setSubject(userId.toString())
                .claim("_time", 1058998878)
                .claim("_r", randomKey)
                .claim("_p", Hashing.hmacMd5(randomKey.getBytes()).hashString("PC", Charsets.UTF_8).toString())
                .signWith(SignatureAlgorithm.HS256, "*&%$io&bhex#broker*%^&")
                .compact();
    }

    public static void main(String[] args){
        System.out.println(getToken(91859285801697280L));
    }

}
