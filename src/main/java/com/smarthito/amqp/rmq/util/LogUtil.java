package com.smarthito.amqp.rmq.util;

/**
 * @author yaojunguang at 2021/4/9 7:10 下午
 */
public class LogUtil {

    //错误的循环输出【默认输出位置】
    
    public static String stackError(Exception ex) {
        StringBuilder builder = new StringBuilder();
        builder.append(ex.getMessage()).append("\n");
        StackTraceElement[] error = ex.getStackTrace();
        for (StackTraceElement stackTraceElement : error) {
            builder.append(stackTraceElement.toString()).append("\n");
        }
        return builder.toString();
    }

}
