package com.pvub.reactivewebapi;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom Metrics
 * @author Udai
 */
public class Metrics {
    private     AtomicLong      m_pending_requests      = new AtomicLong(0);
    private     AtomicLong      m_worker_queue_size     = new AtomicLong(0);
    private     AtomicLong      m_completed_requests    = new AtomicLong(0);
    private     AtomicLong      m_async_requests        = new AtomicLong(0);
    private     AtomicLong      m_avg_latency           = new AtomicLong(0);
    private     Integer         m_workers               = 0;
    private     JsonObject      m_json                  = new JsonObject();
    
    public Metrics() {}
    
    public long addPendingRequest(int count) {
        return m_pending_requests.addAndGet(count);
    }
    public long addPendingRequest() {
        return m_pending_requests.incrementAndGet();
    }
    public long removePendingRequest() {
        return m_pending_requests.decrementAndGet();
    }
    public long getCompletedCount() {
        return m_completed_requests.get();
    }
    public long incrementCompletedCount() {
        return m_completed_requests.incrementAndGet();
    }
    public long resetCompletedCount() {
        return m_completed_requests.getAndSet(0);
    }
    public void setWorkerQueueSize(long tasks) {
        m_worker_queue_size.set(tasks);
    }
    public long resetWorkerQueueSize() {
        return m_worker_queue_size.getAndSet(0);
    }
    public void setAsyncRequests(long async) {
        m_async_requests.set(async);
    }
    public void setAverageLatency(long lat) {
        m_avg_latency.set(lat);
    }
    public void setWorkers(Integer count) {
        m_workers = count;
    }
    @Override
    public String toString() {
        m_json.put("pending", m_pending_requests.get());
        m_json.put("completed", m_completed_requests.get());
        m_json.put("queue", m_worker_queue_size.get());
        m_json.put("async", m_async_requests.get());
        m_json.put("avg_lat", m_avg_latency.get());
        m_json.put("workers", m_workers);
        return m_json.encode();
    }
}
