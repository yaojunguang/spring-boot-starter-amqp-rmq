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
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
 * ??????redis?????????????????????
 *
 * @author yaojunguang at 2021/2/3 5:47 ??????
 */

@Slf4j
@ConditionalOnProperty(name = "spring.redis.rmq.enable", havingValue = "true")
public class RmqAnnotationConfiguration implements BeanPostProcessor {

    @Bean
    @ConditionalOnMissingBean
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
     * ?????????
     */
    @Resource
    private RedisConnectionFactory redisConnectionFactory;

    /**
     * redis template
     */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * ???????????????
     */
    @Resource
    private ThreadPoolTaskScheduler taskScheduler;

    /**
     * ?????????????????????
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
                // ??????????????????????????????????????????
                initContainer(handler.batchSize());
                //???????????????
                String topic = rmqProperties.getStreamPrefix() + handler.topic();
                //?????????
                String consumerName = getConsumerName();
                //?????????????????????
                initTopic(topic);
                //????????????????????????
                StreamListener<String, MapRecord<String, String, String>> streamListener = entries -> {
                    String threadName = Thread.currentThread().getName();
                    RecordId id = entries.getId();
                    String newThreadName = topic + "-" + handler.consumerGroup() + "-" + id.getValue();
                    Thread.currentThread().setName(newThreadName);
                    String key = null;
                    if (rmqProperties.getConsumerLockTime() > 0) {
                        key = "lock:" + newThreadName;
                        if (!Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(key, "", rmqProperties.getConsumerLockTime(), TimeUnit.MINUTES))) {
                            log.info("rmq ???????????? msg={}", entries);
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

                //???????????????
                addConsumerGroup(topic, handler.consumerGroup());
                //??????????????????
                StreamReadOptions readOptions = StreamReadOptions.empty();
                readOptions.block(Duration.ZERO);
                readOptions.count(handler.batchSize());

                container.receive(Consumer.from(handler.consumerGroup(), consumerName),
                        StreamOffset.create(topic, ReadOffset.lastConsumed()),
                        streamListener, readOptions);
                container.start();
                //??????pending??????
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
                            // ?????????????????????handler
                            .errorHandler(t -> log.error("rmq error={}", t.getMessage()))
                            .batchSize(batchSize)
                            // ????????????????????????0????????????????????????????????????????????????
                            .pollTimeout(Duration.ZERO)
                            .serializer(new StringRedisSerializer())
                            .build();
            container = new DefaultStreamMessageListenerContainer<>(redisConnectionFactory, options, rmqProperties.getSleepPerFetch());
        }
    }

    /**
     * ???????????????
     *
     * @param topic         ????????????
     * @param consumerGroup ???????????????
     */
    public void addConsumerGroup(String topic, String consumerGroup) {
        //???????????????group
        try {
            if (stringRedisTemplate.opsForStream().groups(topic).stream().noneMatch(xInfoGroup -> xInfoGroup.groupName().equals(consumerGroup))) {
                stringRedisTemplate.opsForStream().createGroup(topic, consumerGroup);
            }
        } catch (Exception e) {
            if (Objects.requireNonNull(e.getCause()).getClass().equals(RedisBusyException.class)) {
                System.out.println("??????????????????");
            } else {
                throw e;
            }
        }
    }

    /**
     * ?????????????????????
     *
     * @param topic ????????????
     */
    private void initTopic(String topic) {
        //??????????????????stream ??????
        if (!Boolean.TRUE.equals(stringRedisTemplate.hasKey(topic))) {
            //????????????????????????????????????
            ObjectRecord<String, ?> record = StreamRecords.newRecord()
                    .ofObject("")
                    .withStreamKey(topic);
            RecordId recordId = stringRedisTemplate.opsForStream().add(record);
            // ????????????
            log.info("????????????={}", stringRedisTemplate.opsForStream().delete(topic, recordId));
        }
    }

    /**
     * ?????????????????????
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
                    log.error("??????????????????????????????~");
                    uuid = "consumer-0";
                }
            }
            log.info("rmq consumer name:{}", uuid);
        }
        return uuid;
    }

    /**
     * ???????????????ack?????????
     *
     * @param topic         ??????
     * @param consumerGroup ?????????
     * @param consumerName  ?????????
     */
    private void initPendingHandle(String topic, String consumerGroup, String consumerName, StreamListener<String, MapRecord<String, String, String>> streamListener) {
        String pendingKey = RedisAutoConfig.REDIS_LOCK_PREFIX + topic + ":" + consumerGroup + ":pending";
        //??????pending
        taskScheduler.schedule(() -> {
            //??????????????????,??????XX???
            if (Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(pendingKey, "", rmqProperties.getPendingHandleLock(), TimeUnit.SECONDS))) {
                PendingMessage message = stringRedisTemplate.opsForStream()
                        .pending(topic, consumerGroup, Range.closed("0", "+"), 5L)
                        .stream().filter(msg -> {
                            if (msg.getTotalDeliveryCount() > rmqProperties.getPendingRetry()) {
                                //????????????3????????????
                                log.error("rmq ?????????{}????????????ack topic={},msg={},ack={}", rmqProperties.getPendingRetry(), topic, msg,
                                        stringRedisTemplate.opsForStream().acknowledge(topic, msg.getGroupName(), msg.getId()));
                                return false;
                            }
                            //????????????
                            return msg.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofSeconds(rmqProperties.getPending())) > 0;
                        }).findFirst().orElse(null);
                if (message != null) {
                    //??????xClaim?????????????????????????????????????????????????????????
                    try {
                        List<ByteRecord> retVal = stringRedisTemplate.execute((RedisCallback<List<ByteRecord>>) connection -> {
                            log.info("?????????????????????topic={},consumerGroup={},consumerName={},msgId={}", topic, consumerGroup, consumerName, message.getId());
                            RedisStreamCommands.XClaimOptions options = RedisStreamCommands.XClaimOptions.minIdle(Duration.ofSeconds(10)).ids(message.getId());
                            options = options.idle(Duration.ofSeconds(10));
                            return connection.streamCommands().xClaim(topic.getBytes(), consumerGroup, consumerName, options);
                        });
                        if (retVal != null) {
                            for (ByteRecord byteRecord : retVal) {
                                log.info("???????????????????????????id={}, value={}", byteRecord.getId(), byteRecord.getValue());
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    StreamOperations<String, String, String> streamOperations = this.stringRedisTemplate.opsForStream();
                    List<MapRecord<String, String, String>> result = streamOperations.range(topic, Range.rightOpen(message.getId().getValue(), message.getId().getValue()));
                    if (result != null && result.size() == 1) {
                        MapRecord<String, String, String> record = result.get(0);
                        log.info("????????????pending??????, topic={},group={},msg={}", topic, consumerGroup, record);
                        streamListener.onMessage(record);
                    }
                }
                //???????????????????????????????????????????????????0.8
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
