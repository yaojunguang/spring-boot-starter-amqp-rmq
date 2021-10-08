package com.smarthito.amqp.rmq.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yaojunguang at 2021/1/7 5:35 下午
 */

@Slf4j
public class RetryUtil {

    public static <T> T retryAction(RetryUtilInterface<T> executorInterface) {
        return retryAction(executorInterface, 3, 0);
    }

    public static <T> T retryAction(RetryUtilInterface<T> executorInterface, int tryNum) {
        return retryAction(executorInterface, tryNum, 0);
    }

    public static <T> T retryAction(RetryUtilInterface<T> executorInterface, int tryNum, long delayMillis) {
        return retryAction(executorInterface, tryNum, delayMillis, true);
    }

    /**
     * 重试封装，返回null重试
     *
     * @param executorInterface 执行函数
     * @param tryNum            重试次数默认3次
     * @param delayMillis       每次重试延迟，默认为0,小于5的不考虑
     * @param logEnable         是否记录日志
     * @param <T>               返回的对象，null来判定结果是否正确
     * @return 返回
     */
    public static <T> T retryAction(RetryUtilInterface<T> executorInterface, int tryNum, long delayMillis, boolean logEnable) {
        int count = 1;
        do {
            try {
                return executorInterface.execute();
            } catch (Exception exception) {
                if (count < tryNum) {
                    if (delayMillis > 5) {
                        try {
                            Thread.sleep(delayMillis);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                } else {
                    if (logEnable && StringUtils.isNotBlank(exception.getMessage())) {
                        log.error("retry info msg={}", exception.getMessage());
                    }
                }
                count++;
            }
        } while (count <= tryNum);
        return null;
    }

    public static <T> T retry(RetryUtilInterface<T> executorInterface) throws Exception {
        return retry(executorInterface, 3, 0);
    }

    public static <T> T retry(RetryUtilInterface<T> executorInterface, int tryNum) throws Exception {
        return retry(executorInterface, tryNum, 0);
    }

    /**
     * 重试封装，返回null重试
     *
     * @param executorInterface 执行函数
     * @param tryNum            重试次数默认3次
     * @param delayMillis       每次重试延迟，默认为0,小于5的不考虑
     * @param <T>               返回的对象，null来判定结果是否正确
     * @return 返回
     * @throws Exception 异常
     */
    public static <T> T retry(RetryUtilInterface<T> executorInterface, int tryNum, long delayMillis) throws Exception {
        int count = 1;
        do {
            try {
                return executorInterface.execute();
            } catch (RetryException retryException) {
                if (count < tryNum) {
                    if (delayMillis > 5) {
                        try {
                            Thread.sleep(delayMillis);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                } else {
                    if (StringUtils.isNotBlank(retryException.getMessage())) {
                        log.error("retry info msg={}", retryException.getMessage());
                    }
                    throw retryException;
                }
                count++;
            }
        } while (count <= tryNum);
        return null;
    }

    public static class RetryException extends RuntimeException {

        public RetryException() {
            super();
        }

        public RetryException(String msg) {
            super(msg);
        }
    }
}
