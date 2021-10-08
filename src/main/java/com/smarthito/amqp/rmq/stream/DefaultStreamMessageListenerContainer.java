package com.smarthito.amqp.rmq.stream;


import com.smarthito.amqp.rmq.config.RedisAutoConfig;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.stream.*;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Simple {@link Executor} based {@link StreamMessageListenerContainer} implementation for running {@link Task tasks} to
 * poll on Redis Streams.
 * This message container creates long-running tasks that are executed on {@link Executor}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
@Slf4j
public class DefaultStreamMessageListenerContainer<K, V extends Record<K, ?>> implements StreamMessageListenerContainer<K, V> {

    private final Object lifecycleMonitor = new Object();
    private final StreamReadOptions readOptions;
    private final Executor taskExecutor;
    private final ErrorHandler errorHandler;
    private final RedisTemplate<K, ?> template;
    private final StreamOperations<K, Object, Object> streamOperations;
    private final StreamMessageListenerContainerOptions<K, V> containerOptions;

    /**
     * 每次拉取后休眠时间
     */
    private long sleepPerFetch;

    public void setSleepPerFetch(long sleepPerFetch) {
        this.sleepPerFetch = sleepPerFetch;
    }

    /**
     * 延迟消息处理脚本，统一的，仅需要一次处理
     */
    private static String delayLuaScript = null;

    private final List<Subscription> subscriptions = new ArrayList<>();

    private boolean running = false;

    /**
     * Create a new {@link DefaultStreamMessageListenerContainer}.
     *
     * @param connectionFactory must not be {@literal null}.
     * @param containerOptions  must not be {@literal null}.
     * @param sleepPerFetch     执行后空消息后休眠.
     */
    public DefaultStreamMessageListenerContainer(RedisConnectionFactory connectionFactory,
                                                 StreamMessageListenerContainerOptions<K, V> containerOptions,
                                                 long sleepPerFetch) {

        Assert.isTrue(sleepPerFetch > 0, "sleepPerFetch must be greater zero!");
        Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null!");
        Assert.notNull(containerOptions, "StreamMessageListenerContainerOptions must not be null!");

        this.taskExecutor = containerOptions.getExecutor();
        this.errorHandler = containerOptions.getErrorHandler();
        this.readOptions = getStreamReadOptions(containerOptions);
        this.template = createRedisTemplate(connectionFactory, containerOptions);
        this.containerOptions = containerOptions;
        this.sleepPerFetch = sleepPerFetch;

        if (containerOptions.getHashMapper() != null) {
            this.streamOperations = this.template.opsForStream(containerOptions.getHashMapper());
        } else {
            this.streamOperations = this.template.opsForStream();
        }
    }

    private static StreamReadOptions getStreamReadOptions(StreamMessageListenerContainerOptions<?, ?> options) {

        StreamReadOptions readOptions = StreamReadOptions.empty();

        if (options.getBatchSize().isPresent()) {
            readOptions = readOptions.count(options.getBatchSize().getAsInt());
        }

        if (!options.getPollTimeout().isZero()) {
            readOptions = readOptions.block(options.getPollTimeout());
        }

        return readOptions;
    }

    private RedisTemplate<K, V> createRedisTemplate(RedisConnectionFactory connectionFactory,
                                                    StreamMessageListenerContainerOptions<K, V> containerOptions) {
        RedisTemplate<K, V> template = new RedisTemplate<>();
        template.setKeySerializer(containerOptions.getKeySerializer());
        template.setValueSerializer(containerOptions.getKeySerializer());
        template.setHashKeySerializer(containerOptions.getHashKeySerializer());
        template.setHashValueSerializer(containerOptions.getHashValueSerializer());
        template.setConnectionFactory(connectionFactory);
        template.afterPropertiesSet();
        return template;
    }

    @Override
    public boolean isAutoStartup() {
        return false;
    }

    @Override
    public void stop(Runnable callback) {

        stop();
        callback.run();
    }

    @Override
    public void start() {

        synchronized (lifecycleMonitor) {

            if (this.running) {
                return;
            }

            subscriptions.stream()
                    .filter(it -> !it.isActive())
                    .filter(it -> it instanceof TaskSubscription)
                    .map(TaskSubscription.class::cast)
                    .map(TaskSubscription::getTask)
                    .forEach(taskExecutor::execute);

            running = true;
        }
    }

    @Override
    public void stop() {
        synchronized (lifecycleMonitor) {
            if (this.running) {
                subscriptions.forEach(Cancelable::cancel);
                running = false;
            }
        }
    }

    @Override
    public boolean isRunning() {

        synchronized (this.lifecycleMonitor) {
            return running;
        }
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Subscription receive(Consumer consumer, StreamOffset<K> streamOffset, StreamListener<K, V> listener) {
        return register(StreamReadRequest.builder(streamOffset).consumer(consumer).autoAcknowledge(false).build(),
                listener);
    }

    public Subscription receive(Consumer consumer, StreamOffset<K> streamOffset, StreamListener<K, V> listener, StreamReadOptions readOptions) {
        return register(StreamReadRequest.builder(streamOffset).consumer(consumer).autoAcknowledge(false).build(),
                listener, readOptions);
    }

    @Override
    public Subscription register(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener) {
        return doRegister(getReadTask(streamRequest, listener, this.readOptions));
    }

    public Subscription register(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener, StreamReadOptions readOptions) {
        return doRegister(getReadTask(streamRequest, listener, readOptions));
    }

    @SuppressWarnings("unchecked")
    private StreamPollTask<K, V> getReadTask(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener, StreamReadOptions readOptions) {

        BiFunction<K, ReadOffset, List<? extends Record<?, ?>>> readFunction = getReadFunction(streamRequest, readOptions);

        return new StreamPollTask<K, V>(streamRequest, listener, errorHandler, (BiFunction) readFunction, sleepPerFetch, getDelayFunction());
    }

    @SuppressWarnings("unchecked")
    private BiFunction<K, ReadOffset, List<? extends Record<?, ?>>> getReadFunction(StreamReadRequest<K> streamRequest, StreamReadOptions options) {

        if (streamRequest instanceof StreamMessageListenerContainer.ConsumerStreamReadRequest) {

            ConsumerStreamReadRequest<K> consumerStreamRequest = (ConsumerStreamReadRequest<K>) streamRequest;

            StreamReadOptions readOptions = consumerStreamRequest.isAutoAcknowledge() ? options.autoAcknowledge()
                    : options;
            Consumer consumer = consumerStreamRequest.getConsumer();

            if (this.containerOptions.getHashMapper() != null) {
                return (key, offset) -> streamOperations.read(this.containerOptions.getTargetType(), consumer, readOptions,
                        StreamOffset.create(key, offset));
            }

            return (key, offset) -> streamOperations.read(consumer, readOptions, StreamOffset.create(key, offset));
        }

        if (this.containerOptions.getHashMapper() != null) {
            return (key, offset) -> streamOperations.read(this.containerOptions.getTargetType(), options,
                    StreamOffset.create(key, offset));
        }

        return (key, offset) -> streamOperations.read(options, StreamOffset.create(key, offset));
    }

    /**
     * 获取延迟处理脚本
     *
     * @return 结果
     */
    private DelayFunction<K> getDelayFunction() {

        //准备执行脚本
        if (delayLuaScript == null) {
            DefaultRedisScript<String> redisScript = getDelayScript();
            //预加载调用的脚本
            delayLuaScript = Objects.requireNonNull(template.getConnectionFactory()).getConnection().scriptLoad(redisScript.getScriptAsString().getBytes());
            log.info("rmq delay script load sha={}", delayLuaScript);
        }
        return key -> {
            String result = null;
            if (delayLuaScript != null) {
                //有预加载脚本
                byte[] resultByte = Objects.requireNonNull(template.getConnectionFactory()).getConnection().evalSha(delayLuaScript, ReturnType.VALUE,
                        1, String.valueOf(key).getBytes(), RedisAutoConfig.DELAY_END_WITH.getBytes(), String.valueOf(System.currentTimeMillis()).getBytes());
                if (resultByte != null) {
                    result = new String(resultByte);
                }
            } else {
                //无预加载脚本方式
                DefaultRedisScript<String> redisScript = getDelayScript();
                result = template.execute(redisScript, Collections.singletonList(key), RedisAutoConfig.DELAY_END_WITH, String.valueOf(System.currentTimeMillis()));
            }
            if (result != null && !"{}".equals(result)) {
                log.info("delay handle topic={},result={}", key, result);
            }
        };
    }

    private DefaultRedisScript<String> getDelayScript() {
        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("redis/rmq_delay.lua")));
        redisScript.setResultType(String.class);
        return redisScript;
    }

    private Subscription doRegister(Task task) {

        Subscription subscription = new TaskSubscription(task);

        synchronized (lifecycleMonitor) {

            this.subscriptions.add(subscription);

            if (this.running) {
                taskExecutor.execute(task);
            }
        }

        return subscription;
    }

    @Override
    public void remove(Subscription subscription) {

        synchronized (lifecycleMonitor) {

            if (subscriptions.contains(subscription)) {

                if (subscription.isActive()) {
                    subscription.cancel();
                }

                subscriptions.remove(subscription);
            }
        }
    }

    /**
     * {@link Subscription} wrapping a {@link Task}.
     *
     * @author Mark Paluch
     * @since 2.2
     */
    @EqualsAndHashCode
    @RequiredArgsConstructor
    static class TaskSubscription implements Subscription {

        private final Task task;

        Task getTask() {
            return task;
        }

        @Override
        public boolean isActive() {
            return task.isActive();
        }

        @Override
        public boolean await(Duration timeout) throws InterruptedException {
            return task.awaitStart(timeout);
        }

        @Override
        public void cancel() throws DataAccessResourceFailureException {
            task.cancel();
        }
    }

    /**
     * Logging {@link ErrorHandler}.
     *
     * @author Mark Paluch
     * @since 2.2
     */
    enum LoggingErrorHandler implements ErrorHandler {

        //单例
        INSTANCE;

        private final Log logger;

        LoggingErrorHandler() {
            this.logger = LogFactory.getLog(LoggingErrorHandler.class);
        }

        @Override
        public void handleError(@NotNull Throwable t) {

            if (this.logger.isErrorEnabled()) {
                this.logger.error("Unexpected error occurred in scheduled task.", t);
            }
        }
    }
}
