package com.smarthito.amqp.rmq.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author yaojunguang at 2021/3/25 6:37 下午
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RmqTopic {

    /**
     * 消息组名称
     *
     * @return 名称
     */
    String value() default "default";
}
