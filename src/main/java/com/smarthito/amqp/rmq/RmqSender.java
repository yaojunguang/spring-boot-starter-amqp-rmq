package com.smarthito.amqp.rmq;

import com.smarthito.amqp.rmq.config.RedisAutoConfig;
import com.smarthito.amqp.rmq.util.JsonUtil;
import com.smarthito.amqp.rmq.util.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * redis 5.0以上支持stream
 *
 * @author yaojunguang at 2021/3/25 6:45 下午
 */

@Slf4j
public class RmqSender {

    private final StringRedisTemplate stringRedisTemplate;
    private final String topic;
    private final String streamPrefix;

    public RmqSender(String topic, StringRedisTemplate redisTemplate, String streamPrefix) {
        this.stringRedisTemplate = redisTemplate;
        this.topic = topic;
        this.streamPrefix = streamPrefix;
    }

    /**
     * 发送对象，支持基础对象
     *
     * @param object 对象
     * @return 发送的id，null为失败
     */
    public String sendMsg(Object object) {
        return sendMsg(object, 0);
    }

    /**
     * 发送对象，支持基础对象
     *
     * @param object 对象
     * @param delay  延迟时间(S)
     * @return 结果
     */
    public String sendMsg(Object object, int delay) {
        return sendMsg(this.topic, object, delay);
    }


    /**
     * 发送对象，支持基础对象
     *
     * @param topic  消息队列
     * @param object 消息体
     * @return 结果
     */
    public String sendMsg(String topic, Object object) {
        return sendMsg(topic, object, 0);
    }

    /**
     * 发送对象，支持基础对象
     *
     * @param topic  消息队列
     * @param object 对象
     * @param delay  延迟时间 (s)
     * @return 发送的id，null为失败
     */
    public String sendMsg(String topic, Object object, int delay) {
        String data = JsonUtil.toJsonString(object);
        if (delay < 1) {
            //小于1s的延迟，忽略
            ObjectRecord<String, ?> record = StreamRecords.newRecord()
                    .ofObject(data)
                    .withStreamKey(streamPrefix + topic);
            RecordId recordId = RetryUtil.retryAction(() -> {
                RecordId id = stringRedisTemplate.opsForStream().add(record);
                if (id == null) {
                    throw new RetryUtil.RetryException();
                }
                return id;
            }, 3, 10);
            if (recordId != null) {
                String id = recordId.getValue();
                log.info("rmq send topic={},id={},msg={}", topic, id, data);
                return id;
            } else {
                log.error("rmq send fail topic={},msg={}", topic, data);
                return null;
            }
        } else {
            Long increment = stringRedisTemplate.opsForValue().increment(streamPrefix + topic + RedisAutoConfig.DELAY_ID_END_WITH, 1);
            Long score = System.currentTimeMillis() + delay * 1000L;
            String zSetData = increment + "==delayId:" + data;
            stringRedisTemplate.opsForZSet().add(streamPrefix + topic + RedisAutoConfig.DELAY_END_WITH, zSetData, System.currentTimeMillis() + delay * 1000L);
            log.info("rmq send topic={},delay={}s,delayId={},msg={}", topic, delay, increment, data);
            return String.valueOf(score);
        }
    }

}
