/*
 ************************************
 * @项目名称: openapi
 * @文件名称: ThreadExecutors
 * @Date 2018/08/02
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.executor;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

public class ThreadExecutor {

    public static ThreadPoolTaskExecutor executor(int corePoolSize) {
        int maxPoolSize = corePoolSize * 2;
        int queueCapacity = 1000000;
        String name = "bhex-stress-";
        return initExecutor(corePoolSize, maxPoolSize, queueCapacity, name);
    }

    private static ThreadPoolTaskExecutor initExecutor(int corePoolSize, int maxPoolSize, int queueCapacity, String name) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setKeepAliveSeconds(120);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(name);
        executor.initialize();
        return executor;
    }
}
