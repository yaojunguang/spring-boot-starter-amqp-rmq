package com.smarthito.amqp.rmq.util;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author yaojunguang at 2021/1/8 5:10 下午
 */

@Slf4j
public class RandomUtil {

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
            log.error("获取linux/unix系統cpuId发生异常。原因：", e);
            return null;
        }
    }
}
