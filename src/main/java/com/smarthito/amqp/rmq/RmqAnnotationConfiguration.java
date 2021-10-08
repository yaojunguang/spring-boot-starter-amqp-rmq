package com.smarthito.amqp.rmq;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.smarthito.amqp.rmq.annotation.RmqHandler;
import com.smarthito.amqp.rmq.bean.RmqProperties;
import com.smarthito.amqp.rmq.config.RedisAutoConfig;
import com.smarthito.amqp.rmq.stream.DefaultStreamMessageListenerContainer;
import com.smarthito.amqp.rmq.util.JsonUtil;
import com.smarthito.amqp.rmq.util.LogUtil;
import com.smarthito.amqp.rmq.util.NetUtil;
import com.smarthito.amqp.rmq.util.RandomUtil;
import io.lettuce.core.RedisBusyException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.ReflectionUtils;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 加入redis消息队列的方法
 *
 * @author yaojunguang at 2021/2/3 5:47 下午
 */

@Slf4j
@ConditionalOnProperty(name = "spring.redis.rmq.enable", havingValue = "true")
public class RmqAnnotationConfiguration implements BeanPostProcessor {

    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskExecutor() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("stream-container-pool-%d").build();
        ThreadPoolTaskScheduler executor = new ThreadPoolTaskScheduler();
        executor.setPoolSize(20);
        executor.setThreadFactory(namedThreadFactory);
        return executor;
    }

    @Resource
    private RmqProperties rmqProperties;

    /**
     * 连接池
     */
    @Resource
    private RedisConnectionFactory redisConnectionFactory;

    /**
     * redis template
     */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 拉取线程池
     */
    @Resource
    private ThreadPoolTaskScheduler taskScheduler;

    /**
     * 处理容器，通用
     */
    private static DefaultStreamMessageListenerContainer<String, MapRecord<String, String, String>> container;

    @Override
    public Object postProcessBeforeInitialization(@NotNull Object bean, @NotNull String beanName) throws BeansException {
        return bean;
    }

    @SneakyThrows
    @Override
    public Object postProcessAfterInitialization(Object bean, @NotNull String beanName) throws BeansException {
        Method[] methods = ReflectionUtils.getAllDeclaredMethods(bean.getClass());
        for (Method method : methods) {
            RmqHandler handler = AnnotationUtils.findAnnotation(method, RmqHandler.class);
            if (handler != null) {
                // 根据配置对象创建监听容器对象
                initContainer(handler.batchSize());
                //消费的主题
                String topic = rmqProperties.getStreamPrefix() + handler.topic();
                //消费者
                String consumerName = getConsumerName();
                //初始化主题队列
                initTopic(topic);
                //创建主题的消费者
                StreamListener<String, MapRecord<String, String, String>> streamListener = entries -> {
                    String threadName = Thread.currentThread().getName();
                    RecordId id = entries.getId();
                    String newThreadName = topic + "-" + handler.consumerGroup() + "-" + id.getValue();
                    Thread.currentThread().setName(newThreadName);
                    String key = null;
                    if (rmqProperties.getConsumerLockTime() > 0) {
                        key = "lock:" + newThreadName;
                        if (!Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(key, "", rmqProperties.getConsumerLockTime(), TimeUnit.MINUTES))) {
                            log.info("rmq 重复消息 msg={}", entries);
                            return;
                        }
                    }
                    String messageValue = entries.getValue().get("payload");
                    try {
                        log.info("rmq consume id={},msg={}", id, messageValue);
                        Object object = JsonUtil.parseObject(messageValue, method.getParameterTypes()[0]);
                        Object[] parameters = {object};
                        ReflectionUtils.invokeMethod(method, bean, parameters);
                        stringRedisTemplate.opsForStream().acknowledge(topic, handler.consumerGroup(), id);
                    } catch (Exception e) {
                        log.error("rmq consume fail id={},msg={},ex={}", id, messageValue, LogUtil.stackError(e));
                    } finally {
                        if (key != null && rmqProperties.getConsumerLockTime() > 0) {
                            stringRedisTemplate.delete(key);
                        }
                        Thread.currentThread().setName(threadName);
                    }
                };

                //添加消费组
                addConsumerGroup(topic, handler.consumerGroup());
                //设定消费参数
                StreamReadOptions readOptions = StreamReadOptions.empty();
                readOptions.block(Duration.ZERO);
                readOptions.count(handler.batchSize());

                container.receive(Consumer.from(handler.consumerGroup(), consumerName),
                        StreamOffset.create(topic, ReadOffset.lastConsumed()),
                        streamListener, readOptions);
                container.start();
                //添加pending处理
                initPendingHandle(topic, handler.consumerGroup(), consumerName, streamListener);
            }
        }
        return bean;
    }

    private void initContainer(int batchSize) {
        if (container == null) {
            StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                    StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                            .builder()
                            .executor(taskScheduler)
                            // 消息消费异常的handler
                            .errorHandler(t -> log.error("rmq error={}", t.getMessage()))
                            .batchSize(batchSize)
                            // 超时时间，设置为0，表示不超时（超时后会抛出异常）
                            .pollTimeout(Duration.ZERO)
                            .serializer(new StringRedisSerializer())
                            .build();
            container = new DefaultStreamMessageListenerContainer<>(redisConnectionFactory, options, rmqProperties.getSleepPerFetch());
        }
    }

    /**
     * 添加消费组
     *
     * @param topic         消息主题
     * @param consumerGroup 消费组名称
     */
    public void addConsumerGroup(String topic, String consumerGroup) {
        //创建消费的group
        try {
            if (stringRedisTemplate.opsForStream().groups(topic).stream().noneMatch(xInfoGroup -> xInfoGroup.groupName().equals(consumerGroup))) {
                stringRedisTemplate.opsForStream().createGroup(topic, consumerGroup);
            }
        } catch (Exception e) {
            if (Objects.requireNonNull(e.getCause()).getClass().equals(RedisBusyException.class)) {
                System.out.println("消费组已存在");
            } else {
                throw e;
            }
        }
    }

    /**
     * 初始化消息主题
     *
     * @param topic 消息主题
     */
    private void initTopic(String topic) {
        //检查是否有该stream 存在
        if (!Boolean.TRUE.equals(stringRedisTemplate.hasKey(topic))) {
            //如果不存在就发送一个空的
            ObjectRecord<String, ?> record = StreamRecords.newRecord()
                    .ofObject("")
                    .withStreamKey(topic);
            RecordId recordId = stringRedisTemplate.opsForStream().add(record);
            // 删除创建
            log.info("删除创建={}", stringRedisTemplate.opsForStream().delete(topic, recordId));
        }
    }

    /**
     * 获取消费者名称
     */
    private String getConsumerName() {
        String uuid = null;
        try {
            uuid = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (StringUtils.isEmpty(uuid)) {
            uuid = RandomUtil.getSystemUuid();
            if (StringUtils.isEmpty(uuid)) {
                uuid = NetUtil.getLocalIpAddress();
                if (StringUtils.isEmpty(uuid)) {
                    log.error("无法获取到设备识别号~");
                    uuid = "consumer-0";
                }
            }
            log.info("rmq consumer name:{}", uuid);
        }
        return uuid;
    }

    /**
     * 配置超时未ack的处理
     *
     * @param topic         主题
     * @param consumerGroup 消费组
     * @param consumerName  消费端
     */
    private void initPendingHandle(String topic, String consumerGroup, String consumerName, StreamListener<String, MapRecord<String, String, String>> streamListener) {
        String pendingKey = RedisAutoConfig.REDIS_LOCK_PREFIX + topic + ":" + consumerGroup + ":pending";
        //处理pending
        taskScheduler.schedule(() -> {
            //加一个处理锁,锁定XX秒
            if (Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(pendingKey, "", rmqProperties.getPendingHandleLock(), TimeUnit.SECONDS))) {
                PendingMessage message = stringRedisTemplate.opsForStream()
                        .pending(topic, consumerGroup, Range.closed("0", "+"), 5L)
                        .stream().filter(msg -> {
                            if (msg.getTotalDeliveryCount() > rmqProperties.getRetry()) {
                                //重试超过3次，抛弃
                                log.error("rmq 重试超{}次，强制ack topic={},msg={},ack={}", rmqProperties.getRetry(), topic, msg,
                                        stringRedisTemplate.opsForStream().acknowledge(topic, msg.getGroupName(), msg.getId()));
                                return false;
                            }
                            //超时重试
                            return msg.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofSeconds(rmqProperties.getPending())) > 0;
                        }).findFirst().orElse(null);
                if (message != null) {
                    //通过xClaim，更改消费则，但是还是必须自行再度消费
                    try {
                        List<ByteRecord> retVal = stringRedisTemplate.execute((RedisCallback<List<ByteRecord>>) connection -> {
                            log.info("处理超时消息：topic={},consumerGroup={},consumerName={},msgId={}", topic, consumerGroup, consumerName, message.getId());
                            RedisStreamCommands.XClaimOptions options = RedisStreamCommands.XClaimOptions.minIdle(Duration.ofSeconds(10)).ids(message.getId());
                            options = options.idle(Duration.ofSeconds(10));
                            return connection.streamCommands().xClaim(topic.getBytes(), consumerGroup, consumerName, options);
                        });
                        if (retVal != null) {
                            for (ByteRecord byteRecord : retVal) {
                                log.info("改了消息的消费者：id={}, value={}", byteRecord.getId(), byteRecord.getValue());
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    StreamOperations<String, String, String> streamOperations = this.stringRedisTemplate.opsForStream();
                    List<MapRecord<String, String, String>> result = streamOperations.range(topic, Range.rightOpen(message.getId().getValue(), message.getId().getValue()));
                    if (result != null && result.size() == 1) {
                        MapRecord<String, String, String> record = result.get(0);
                        log.info("开始修复pending消息, topic={},group={},msg={}", topic, consumerGroup, record);
                        streamListener.onMessage(record);
                    }
                }
                //删除超出一定数量的数据，保留最大的0.8
                Long size = stringRedisTemplate.opsForStream().size(topic);
                if (size != null && size > rmqProperties.getBaseKeep()) {
                    stringRedisTemplate.opsForStream().trim(topic, (int) (rmqProperties.getBaseKeep() * 0.8));
                }
                stringRedisTemplate.delete(pendingKey);
            }
        }, new CronTrigger(rmqProperties.getPendingCron()));
    }

    @PreDestroy
    public void destroy() {
        if (container.isRunning()) {
            container.stop();
        }
    }

}
