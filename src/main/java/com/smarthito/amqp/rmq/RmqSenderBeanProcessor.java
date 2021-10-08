package com.smarthito.amqp.rmq;

import com.smarthito.amqp.rmq.annotation.RmqTopic;
import com.smarthito.amqp.rmq.bean.RmqProperties;
import com.smarthito.amqp.rmq.config.AnnotationInstantConfigBeanPostProcessor;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValues;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.ReflectionUtils;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 发送器的反射处理
 *
 * @author yaojunguang at 2021/3/25 6:37 下午
 */

@ConditionalOnProperty(name = "spring.redis.rmq.enable", havingValue = "true")
public class RmqSenderBeanProcessor extends AnnotationInstantConfigBeanPostProcessor {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RmqProperties rmqProperties;

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
