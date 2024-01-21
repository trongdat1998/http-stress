/*
 ************************************
 * @项目名称: openapi
 * @文件名称: UserService
 * @Date 2018/08/02
 * @Author will.zhao@bhex.io
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.stress.service.order;

import io.bhex.broker.stress.service.order.helper.RoundRobinSelector;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class UserService {

    private static List<Integer> ids;

    private static RoundRobinSelector selector;

    static {
        ids = new ArrayList<>();
        int min = 30001;
        int max = 60000;
        for (int i = min; i <= max; i++) {
            ids.add(i);
        }
        selector = new RoundRobinSelector(ids.size() - 1);
    }

    public long getAccountId() {
        return ids.get(selector.next());
    }

}
