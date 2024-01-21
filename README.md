HTTP stress tool
### Match stress api
启动
```
curl "http://localhost:7222/internal/match_perf/start?grpcServer=match-engine.exchange&grpcPort=7040&startAid=130001&count=100&nThreads=10&nTradeThreads=1&basePrice=1800" 
```
关闭
```
curl "http://localhost:7222/internal/match_perf/stop"
```

1. startAid=130001,count=100 表示 130001-130100这100个aid参与测试
2. nThreads表示并发线程
3. nTradeThreads表示负责成交的线程

当前脚本不需要sql支持，可以直接调用测试

### Shard stress api
启动
```
curl "http://localhost:7222/internal/shard_perf4/start?grpcServer=bh-shard-1.bluehelix&grpcPort=7011&orgId=9001&startAid=130001&symbol=ETHUSDT&count=100&nThreads=10&nTradeThreads=1&basePrice=1000"
```
关闭
```
curl "http://localhost:7222/internal/shard_perf4/stop"

```
1. startAid=130001,count=100 表示 130001-130100这100个aid参与测试
2. nThreads表示并发线程
3. nTradeThreads表示负责成交的线程

需要在bh_shard_1数据库的tb_balance表给130001-130100这100个aid增加ETH和USDT的余额


### BH-server stress api
启动
```
curl "http://localhost:7222/internal/shard_perf4/start?grpcServer=bh-server.bluehelix&grpcPort=7011&orgId=9001&startAid=130001&symbol=ETHUSDT&count=100&nThreads=10&nTradeThreads=1&basePrice=1000"
```
关闭
```
curl "http://localhost:7222/internal/shard_perf4/stop"

```
1. startAid=130001,count=100 表示 130001-130100这100个aid参与测试
2. nThreads表示并发线程
3. nTradeThreads表示负责成交的线程

需要在bh_server数据库的tb_account增加130001-130100这100个aid的账户信息
需要在bh_shard_1数据库的tb_balance表给130001-130100这100个aid增加ETH和USDT的余额


### Open-API stress api
启动
```
curl "http://localhost:7222/internal/openapi_perf/start?grpcServer=open-api&grpcPort=7128&orgId=9001&startAid=130001&symbol=ETHUSDT&count=100&nThreads=100&nTradeThreads=1&basePrice=1000"
```
关闭
```
curl "http://localhost:7222/internal/openapi_perf/stop"
```
1. startAid=130001,count=100 表示 130001-130100这100个aid参与测试
2. nThreads表示并发线程
3. nTradeThreads表示负责成交的线程

需要修改OpenAPI的鉴权testOpenApiInterceptor，跳过这些测试aid的调用
需要在broker数据库里面tb_user增加230001-230100这100个uid的用户，tb_account增加130001-130100这100个aid的账户信息
需要在bh_server数据库的tb_account增加130001-130100这100个aid的账户信息
需要在bh_shard_1数据库的tb_balance表给130001-130100这100个aid增加ETH和USDT的余额

