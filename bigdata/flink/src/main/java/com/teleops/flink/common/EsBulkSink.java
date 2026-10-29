package com.teleops.flink.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件名: bigdata/flink/src/main/java/com/teleops/flink/common/EsBulkSink.java
 * 所属模块: 模块 19/20 与场景一二共用 —— Elasticsearch Bulk Sink（教学实现）
 * 说明:
 *   - 攒批（10 条或 2 秒）+ POST /_bulk。刻意朴素：让学生看清
 *     "Sink = 攒批 + 刷出 + 容错"三件事；
 *   - 生产对比：官方 flink-connector-elasticsearch 提供 flushOnCheckpoint
 *     （与 Checkpoint 对齐刷出，Exactly-Once 友好）、退避重试、BulkProcessor。
 */
public class EsBulkSink extends RichSinkFunction<String> {
    private final String esUrl;
    private transient List<String> buffer;
    private transient long lastFlush;

    public EsBulkSink(String esUrl) { this.esUrl = esUrl; }

    @Override
    public void open(Configuration parameters) {
        buffer = new ArrayList<>();
        lastFlush = System.currentTimeMillis();
    }

    @Override
    public void invoke(String alert, Context context) {
        buffer.add("{\"index\":{\"_index\":\"telecom-alerts\"}}");
        buffer.add(alert);
        if (buffer.size() >= 10 || System.currentTimeMillis() - lastFlush > 2000) {
            flush();
        }
    }

    private void flush() {
        if (buffer.isEmpty()) return;
        String body = String.join("\n", buffer) + "\n";
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(esUrl).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setRequestProperty("Content-Type", "application/x-ndjson");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes("UTF-8"));
            }
            int code = conn.getResponseCode();
            if (code != 200) {
                System.err.println("ES bulk 非正常响应: " + code);
            }
            conn.disconnect();
        } catch (Exception e) {
            // 教学实现：失败打印（生产需重试队列 + 死信，见模块 20 文档）
            System.err.println("ES bulk 写入失败: " + e.getMessage());
        } finally {
            buffer.clear();
            lastFlush = System.currentTimeMillis();
        }
    }

    @Override
    public void close() { flush(); }
}
