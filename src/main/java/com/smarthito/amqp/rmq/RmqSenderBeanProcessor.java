package com.smarthito.amqp.rmq;

import com.smarthito.amqp.rmq.annotation.RmqTopic;
import com.smarthito.amqp.rmq.bean.RmqProperties;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.config.SmartInstantiationAwareBeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.ReflectionUtils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 发送器的反射处理
 *
 * @author yaojunguang at 2021/3/25 6:37 下午
 */

@ConditionalOnProperty(name = "spring.data.redis.rmq.enable", havingValue = "true")
public class RmqSenderBeanProcessor implements SmartInstantiationAwareBeanPostProcessor {

    private final StringRedisTemplate stringRedisTemplate;

    private final RmqProperties rmqProperties;

    @Lazy
    public RmqSenderBeanProcessor(RmqProperties rmqProperties, StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.rmqProperties = rmqProperties;
    }

    /**
     * 发送端集合
     */
    private final static ConcurrentHashMap<String, RmqSender> SENDERS = new ConcurrentHashMap<>();

    @Override
    public PropertyValues postProcessProperties(@NotNull PropertyValues pvs, Object bean, @NotNull String beanName) throws BeansException {
        ReflectionUtils.doWithFields(bean.getClass(), field -> {
            RmqTopic wos = field.getAnnotation(RmqTopic.class);
            String name = wos.value();
            String clientBeanName = "RmqSender_" + name;
            RmqSender sender = SENDERS.getOrDefault(clientBeanName, null);
            if (sender == null) {
                sender = new RmqSender(name, stringRedisTemplate, rmqProperties.getStreamPrefix());
                SENDERS.put(clientBeanName, sender);
            }
            field.setAccessible(true);
            try {
                field.set(bean, sender);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

        }, field -> field.isAnnotationPresent(RmqTopic.class));
        return pvs;
    }


}
