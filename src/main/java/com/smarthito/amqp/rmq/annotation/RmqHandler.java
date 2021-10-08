package com.smarthito.amqp.rmq.annotation;

import java.lang.annotation.*;

/**
 * @author yaojunguang at 2021/3/25 3:41 下午
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface RmqHandler {

    /**
     * 消息组名称
     *
     * @return 名称
     */
    String topic() default "default";

    /**
     * 消费组名称
     *
     * @return 名称
     */
    String consumerGroup() default "default";

    /**
     * 每批次拉取
     *
     * @return 结果
     */
    int batchSize() default 1;
}
