package com.smarthito.amqp.rmq.util;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.UUID;

/**
 * @author yaojunguang at 2021/1/8 5:10 下午
 */

@Slf4j
public class RandomUtil {

    /**
     * 顶级处理
     *
     * @return 结果
     */
    public static int random() {
        Random random = new Random();
        return random.nextInt(100);
    }

    public static int random(int upBound) {
        Random random = new Random();
        return random.nextInt(upBound);
    }

    /**
     * 生成随机字符串
     *
     * @param length 字符串长度
     * @return 字符串
     */
    public static String randomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    /**
     * 仅供PanoViewData使用
     *
     * @return 结果
     */
    public static String NewUUID() {
        return randomString(8);
    }

    /**
     * 功能：获取UUID并去除“-”
     */
    public static String getUUID() {
        String uuid = UUID.randomUUID().toString();
        return uuid.replaceAll("\\-", "");
    }

    /**
     * 获取Docker内的系统UUID
     *
     * @return 获取id
     */
    public static String getSystemUuid() {
        StringBuilder result = new StringBuilder();
        Runtime rt = Runtime.getRuntime();
        try {
            Process proc = rt.exec("cat /sys/class/dmi/id/product_uuid");
            InputStreamReader isr = new InputStreamReader(proc.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                result.append(line);
            }
            isr.close();
            result = new StringBuilder(result.toString().trim());
            result = new StringBuilder(result.toString().replace(" ", ""));
            return result.toString();
        } catch (IOException e) {
            log.error("获取linux/unix系統cpuId发生异常。原因：" + e);
            return null;
        }
    }
}
