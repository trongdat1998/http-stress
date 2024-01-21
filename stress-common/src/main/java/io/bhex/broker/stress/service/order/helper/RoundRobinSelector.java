/*
 ************************************
 * @项目名称: openapi
 * @文件名称: RoundRobin
 * @Date 2018/08/02
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.service.order.helper;

import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinSelector {

    private final int maxIndex;

    private AtomicInteger counter = new AtomicInteger(0);

    public RoundRobinSelector(int maxIndex) {
        if (maxIndex <= 0) {
            throw new IllegalArgumentException("maxIndex must be a positive number. " + maxIndex);
        }
        this.maxIndex = maxIndex;
    }

    public int next() {
        while (true) {
            int current = counter.get();
            int next = (current + 1) % maxIndex;
            if (counter.compareAndSet(current, next)) {
                return current;
            }
        }
    }
}
