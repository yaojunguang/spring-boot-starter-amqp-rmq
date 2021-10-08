package com.smarthito.amqp.rmq.stream;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.ConsumerStreamReadRequest;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamReadRequest;
import org.springframework.data.redis.stream.Task;
import org.springframework.util.ErrorHandler;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * {@link Task} that invokes a {@link BiFunction read function} to poll on a Redis Stream.
 *
 * @author Mark Paluch
 * @see 2.2
 */
class StreamPollTask<K, V extends Record<K, ?>> implements Task {

    private final long sleepTime;
    private final StreamReadRequest<K> request;
    private final StreamListener<K, V> listener;
    private final ErrorHandler errorHandler;
    private final Predicate<Throwable> cancelSubscriptionOnError;
    private final BiFunction<K, ReadOffset, List<V>> readFunction;
    private final DelayFunction<K> delayFunction;

    private final PollState pollState;
    private volatile boolean isInEventLoop = false;

    StreamPollTask(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener, ErrorHandler errorHandler,
                   BiFunction<K, ReadOffset, List<V>> readFunction,
                   long sleepTime, DelayFunction<K> delayFunction) {

        this.request = streamRequest;
        this.listener = listener;
        this.errorHandler = Optional.ofNullable(streamRequest.getErrorHandler()).orElse(errorHandler);
        this.cancelSubscriptionOnError = streamRequest.getCancelSubscriptionOnError();
        this.readFunction = readFunction;
        this.delayFunction = delayFunction;
        this.pollState = createPollState(streamRequest);
        this.sleepTime = sleepTime;
    }

    private static PollState createPollState(StreamReadRequest<?> streamRequest) {

        StreamOffset<?> streamOffset = streamRequest.getStreamOffset();

        if (streamRequest instanceof ConsumerStreamReadRequest) {
            return PollState.consumer(((ConsumerStreamReadRequest<?>) streamRequest).getConsumer(), streamOffset.getOffset());
        }

        return PollState.standalone(streamOffset.getOffset());
    }

    @Override
    public void cancel() throws DataAccessResourceFailureException {
        this.pollState.cancel();
    }

    @Override
    public State getState() {
        return pollState.getState();
    }

    @Override
    public boolean awaitStart(Duration timeout) throws InterruptedException {
        return pollState.awaitStart(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean isLongLived() {
        return true;
    }

    @Override
    public void run() {

        pollState.starting();

        try {

            isInEventLoop = true;
            pollState.running();
            doLoop(request.getStreamOffset().getKey());
        } finally {
            isInEventLoop = false;
        }
    }

    private void doLoop(K key) {
        do {
            try {
                //先处理延迟的消息
                if (delayFunction != null) {
                    delayFunction.execute(key);
                }
                //正常的队列处理
                List<V> read = readFunction.apply(key, pollState.getCurrentReadOffset());
                for (V message : read) {
                    listener.onMessage(message);
                    pollState.updateReadOffset(message.getId().getValue());
                }
                //如果上一次取出的数据为0则休眠
                if (read.size() == 0) {
                    // allow interruption
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException e) {
                cancel();
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                if (cancelSubscriptionOnError.test(e)) {
                    cancel();
                }
                errorHandler.handleError(e);
            }
        } while (pollState.isSubscriptionActive());
    }

    @Override
    public boolean isActive() {
        return State.RUNNING.equals(getState()) || isInEventLoop;
    }

    /**
     * Object representing the current polling state for a particular stream subscription.
     */
    static class PollState {

        private final ReadOffsetStrategy readOffsetStrategy;
        private final Optional<Consumer> consumer;
        private volatile ReadOffset currentOffset;
        private volatile State state = State.CREATED;
        private volatile CountDownLatch awaitStart = new CountDownLatch(1);

        private PollState(Optional<Consumer> consumer, ReadOffsetStrategy readOffsetStrategy, ReadOffset currentOffset) {

            this.readOffsetStrategy = readOffsetStrategy;
            this.currentOffset = currentOffset;
            this.consumer = consumer;
        }

        /**
         * Create a new state object for standalone-read.
         *
         * @param offset the {@link ReadOffset} to use.
         * @return new instance of {@link PollState}.
         */
        static PollState standalone(ReadOffset offset) {

            ReadOffsetStrategy strategy = ReadOffsetStrategy.getStrategy(offset);
            return new PollState(Optional.empty(), strategy, strategy.getFirst(offset, Optional.empty()));
        }

        /**
         * Create a new state object for consumergroup-read.
         *
         * @param consumer the {@link Consumer} to use.
         * @param offset   the {@link ReadOffset} to apply.
         * @return new instance of {@link PollState}.
         */
        static PollState consumer(Consumer consumer, ReadOffset offset) {

            ReadOffsetStrategy strategy = ReadOffsetStrategy.getStrategy(offset);
            Optional<Consumer> optionalConsumer = Optional.of(consumer);
            return new PollState(optionalConsumer, strategy, strategy.getFirst(offset, optionalConsumer));
        }

        boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
            return awaitStart.await(timeout, unit);
        }

        public State getState() {
            return state;
        }

        /**
         * @return {@literal true} if the subscription is active.
         */
        boolean isSubscriptionActive() {
            return state == State.STARTING || state == State.RUNNING;
        }

        /**
         * Set the state to {@link State#STARTING}.
         */
        void starting() {
            state = State.STARTING;
        }

        /**
         * Switch the state to {@link State#RUNNING}.
         */
        void running() {

            state = State.RUNNING;

            CountDownLatch awaitStart = this.awaitStart;

            if (awaitStart.getCount() == 1) {
                awaitStart.countDown();
            }
        }

        /**
         * Set the state to {@link State#CANCELLED} and re-arm the
         * {@link #awaitStart(long, TimeUnit) await synchronizer}.
         */
        void cancel() {

            awaitStart = new CountDownLatch(1);
            state = State.CANCELLED;
        }

        /**
         * Advance the {@link ReadOffset}.
         */
        void updateReadOffset(String messageId) {
            currentOffset = readOffsetStrategy.getNext(getCurrentReadOffset(), consumer, messageId);
        }

        ReadOffset getCurrentReadOffset() {
            return currentOffset;
        }
    }
}
