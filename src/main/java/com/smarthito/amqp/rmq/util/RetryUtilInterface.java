package com.smarthito.amqp.rmq.util;

/**
 * @author yaojunguang at 2021/1/7 5:53 下午
 */
public interface RetryUtilInterface<T> {
    /**
     * 执行调用
     *
     * @return 结果
     * @throws Exception ex
     */
    T execute() throws Exception;
}
