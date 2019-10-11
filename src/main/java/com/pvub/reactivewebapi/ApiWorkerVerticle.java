package com.pvub.reactivewebapi;

import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Worker Verticle to process API requests asynchronously
 * @author Udai
 */
public class ApiWorkerVerticle extends AbstractVerticle {
    private Logger          m_logger;
    private ResourceAPI     m_api;
    private Integer         m_max_delay_milliseconds;
    private Subscription    m_metrics_timer_sub;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    private Metrics         m_metrics;
    private AtomicLong      m_latency = new AtomicLong(0);
    private Integer         m_instance = null;
    private JsonObject      m_metrics_obj = new JsonObject();
    
    public ApiWorkerVerticle() {
        super();
    }
    
    @Override
    public void start() throws Exception {
        JsonObject try_config = config();
        if (try_config != null) {
            m_instance = try_config.getInteger("instance", 999);
            m_logger = LoggerFactory.getLogger("WORKER"+m_instance);
        } else {
            m_logger = LoggerFactory.getLogger("WORKER");
        }
        m_metrics = new Metrics();
        
        String path_to_config = System.getProperty("reactiveapi.config", "conf/config.json");
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
            .setType("file")
            .setFormat("json")
            .setConfig(new JsonObject().put("path", path_to_config));
        
        ConfigRetriever retriever = ConfigRetriever.create(vertx, 
                new ConfigRetrieverOptions().addStore(fileStore));
        retriever.getConfig(
            config -> {
                m_logger.info("config retrieved");
                if (config.failed()) {
                    m_logger.info("No config");
                } else {
                    m_logger.info("Got config");
                    startup(config.result());
                }
            }
        );
        
        retriever.listen(change -> {
            m_logger.info("config changed");
            // Previous configuration
            JsonObject previous = change.getPreviousConfiguration();
            // New configuration
            JsonObject conf = change.getNewConfiguration();
        });
        
    }
    
    private void processConfig(JsonObject config) {
        m_config = config;
    }
    
    private void startup(JsonObject config) {
        processConfig(config);
        m_max_delay_milliseconds = m_config.getInteger("max-delay-milliseconds", 1000);
        Integer worker_pool_size = m_config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        m_logger.info("max_delay_milliseconds={} worker_pool_size={}", m_max_delay_milliseconds, worker_pool_size);
        m_worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        m_scheduler = Schedulers.from(m_worker_executor);
        m_api = new ResourceAPI(m_worker_executor, m_max_delay_milliseconds);
        m_api.build();
        MessageConsumer<String> consumer = 
                    vertx.eventBus().consumer("WORKER" + m_instance);

        consumer.handler(m -> {
            getResponse(m);
        });
        
        m_metrics_timer_sub = Observable.interval(1, TimeUnit.SECONDS)
                      .subscribe(delay -> {
                          measure();
                      }, 
                      error -> {
                          m_logger.error("Metrics error", error);
                      }, 
                      () -> {});
    }

    private void measure() {
        m_metrics_obj.put("instance", m_instance);
        int qsize = ((ThreadPoolExecutor)m_worker_executor).getQueue().size();
        m_metrics_obj.put("queue", qsize);
        m_metrics_obj.put("completed", m_metrics.getCompletedCount());
        if (m_metrics.getCompletedCount() > 0) {
            m_metrics_obj.put("avg_lat", m_latency.get() / m_metrics.getCompletedCount());
        } else {
            m_metrics_obj.put("avg_lat", 0);
        }
        
        vertx.eventBus().publish("api.to.client", Json.encode(m_metrics_obj));
        m_metrics.resetCompletedCount();
        m_latency.set(0);
    }
    
    private void getResponse(Message<String> m) {
        String id = m.body();
        int idAsInt = Integer.parseInt(id);
        long startTS = System.nanoTime();
        m_api.fetchResource(idAsInt)
                .subscribeOn(m_scheduler)
                .observeOn(m_scheduler)
                .subscribe(r -> {
                    m_logger.info("Sending response for request {} Outstanding {}", id, m_metrics.removePendingRequest());
                    m_metrics.incrementCompletedCount();
                    m_latency.addAndGet((System.nanoTime() - startTS)/1000000);
                    m.reply(Json.encodePrettily(r));
                }, 
                e -> {
                    m_logger.info("Sending response for request {} Outstanding {}", id, m_metrics.removePendingRequest());
                    m_metrics.incrementCompletedCount();
                    m_latency.addAndGet((System.nanoTime() - startTS)/1000000);
                    m.fail(0, "API Error");
                }, () -> {});
    }
    
    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping WorkerVerticle");
        m_worker_executor.shutdown();
        if (m_metrics_timer_sub != null) {
            m_metrics_timer_sub.unsubscribe();
        }
    }
}
