package com.boya.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Flume ETL拦截器LogETLInterceptor
 */
public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    /**
     * 定义拦截的方法
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {

        // 1 获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        // 2 检验 启动日志（json） 事件日志（服务器时间|json）
        if (log.contains("start")) {
            // 检验启动日志
            if (LogUtils.validateStart(log)) {
                return event;
            }
            // 校验事件日志
        } else {
            if (LogUtils.validateEvent(log)) {
                return event;
            }
        }

        // 3 返回校验结果
        return null;
    }

    /**
     * 将拦截到的数据添加到集合中
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept1 = intercept(event);

            if (intercept1 != null) {
                interceptors.add(intercept1);
            }
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    /**
     * 静态内部类
     */
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}