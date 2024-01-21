/*
 ************************************
 * @项目名称: openapi
 * @文件名称: WebSocketClient
 * @Date 2018/08/02
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.protocol.websocket;

import io.bhex.broker.stress.config.QuoteConfig;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import java.util.concurrent.ExecutionException;

@Slf4j
public class WebSocketClient {

    public WebSocket get(String url) throws ExecutionException, InterruptedException {
        return QuoteConfig.client.prepareGet(url)
                .execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(
                        new WebSocketListener() {
                            @Override
                            public void onOpen(WebSocket websocket) {
                                websocket.sendTextFrame("pong...");
                            }

                            @Override
                            public void onClose(WebSocket websocket, int code, String reason) {
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onTextFrame(String payload, boolean finalFragment, int rsv) {
//                                log.info("payload = {}", payload.length());

                            }
                        }).build()).get();
    }
}
