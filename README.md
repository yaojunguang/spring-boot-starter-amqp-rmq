# 说明

基于 redis stream 改造化的消息队列，需要redis 5.0及以上版本支持

支持对消费组消费，消息支持ack机制，支持配置重试，支持重试延迟配置，支持消息延迟消费

## 使用说明

### maven 依赖

```
    <dependency>
        <groupId>com.smarthito</groupId>
        <artifactId>spring-boot-starter-amqp-rmq</artifactId>
        <version>1.1.0</version>
    </dependency>
```

### redis配置

常规配置，通用jedis、lettuce，redission

### 可选配置

```yaml

spring:
  data:
      redis:
        rmq:
          enable: true #默认需开启
          pending: 300 #超时未ack 消息重试时间延迟
          pending-handle-lock: 20 #超时未ack消息处理时间锁
          consumer-lock-time: 20 # 消息消费时间锁，0为不开启

```

### 使用

#### 发送对象及主题声明

```
@RmqTopic("主题字符串")
private RmqSender rmqSender;
```

#### 发送消息

```
//无延迟
rmqSender.sendMsg(mqEntity);
//带延迟，单位秒,支持消息重排，不必消息一定顺序发送
rmqSender.sendMsg(mqEntity,10);
    
```

#### 消费消息

```
@RmqHandler(topic = "主题字符串")
public void rmqExecuteResult(ResultMqEntity mqEntity)throws Exception{

}

//支持多消费组
@RmqHandler(topic = "主题字符串", consumerGroup = "group2")
public void rmqExecuteResult(ResultMqEntity mqEntity)throws Exception{

}
```
