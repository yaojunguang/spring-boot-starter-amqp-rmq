package com.smarthito.amqp.rmq.stream;

/**
 * @author yaojunguang
 */
public interface DelayFunction<K> {
    /**
     * 具体执行
     *
     * @param key 主key
     */
    void execute(K key);
}
