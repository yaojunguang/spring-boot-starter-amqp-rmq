package com.smarthito.amqp.rmq.bean;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * redis rmq的配置情况
 *
 * @author yaojunguang at 2021/3/26 2:28 下午
 */

@Data
@Configuration
@ConfigurationProperties(prefix = "spring.redis.rmq", ignoreInvalidFields = true)
public class RmqProperties {

    /**
     * 是否开启
     */
    private Boolean enable = false;

    /**
     * 消费未ack时间，秒
     */
    private Integer pending = 300;

    /**
     * 处理pending的定时
     */
    private String pendingCron = "0/5 * * * * ?";

    /**
     * 处理pending的锁定时间
     */
    private Integer pendingHandleLock = 10;

    /**
     * 最大重试次数
     */
    private Integer retry = 3;

    /**
     * 每次拉取的间隔
     */
    private Long sleepPerFetch = 500L;

    /**
     * 基础保留数据量
     */
    private int baseKeep = 50000;

    /**
     * 工作流前缀
     */
    private String streamPrefix = "pano:stream:";

    /**
     * 消费锁的默认时长，min
     */
    private int consumerLockTime = 0;
}
