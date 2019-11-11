package com.pvub.reactivewebapi;

import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageProducer;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import io.vertx.rxjava.ext.web.handler.sockjs.SockJSHandler;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class ApiVerticle extends AbstractVerticle {
    static final int WORKER_POOL_SIZE = 20;

    private final Logger    m_logger;
    private Router          m_router;
    private Integer         m_max_delay_milliseconds;
    private Subscription    m_metrics_timer_sub;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    private Metrics         m_metrics;
    private AtomicLong      m_latency = new AtomicLong(0);
    private Integer         m_worker_count = 0;
    private AtomicInteger   m_current_workers = new AtomicInteger(0);
    private Integer         m_instance_counter = new Integer(1);
    private Stack<String>   m_deployed_verticles = new Stack<String>();
    private AtomicInteger   m_next_worker = new AtomicInteger(1);
    
    public ApiVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("APIVERTICLE");
        m_metrics = new Metrics();
    }
    @Override
    public void start() throws Exception {
        m_logger.info("Starting ApiVerticle");
        
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
            processConfigChange(previous, conf);
        });
    }
    
    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping ApiVerticle");
        m_worker_executor.shutdown();
        if (m_metrics_timer_sub != null) {
            m_metrics_timer_sub.unsubscribe();
        }
    }
    
    private void processConfig(JsonObject config) {
        m_config = config;
        m_worker_count           = m_config.getInteger("worker-count", 1);
        m_max_delay_milliseconds = m_config.getInteger("max-delay-milliseconds", 1000);
        Integer worker_pool_size = m_config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        m_logger.info("max_delay_milliseconds={} worker_pool_size={}", m_max_delay_milliseconds, worker_pool_size);
        if (m_worker_executor == null)
        {
            m_worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        }
        if (m_scheduler == null)
        {
            m_scheduler = Schedulers.from(m_worker_executor);
        }
        m_metrics_timer_sub = Observable.interval(1, TimeUnit.SECONDS)
                      .subscribe(delay -> {
                          measure();
                      }, 
                      error -> {
                          m_logger.error("Metrics error", error);
                      }, 
                      () -> {});
    }
    
    private void processConfigChange(JsonObject prev, JsonObject current) {
    }
    
    private void startup(JsonObject config) {
        processConfig(config);

        // Create a router object.
        m_router = Router.router(vertx);

        // Handle CORS requests.
        m_router.route().handler(CorsHandler.create("*")
            .allowedMethod(HttpMethod.GET)
            .allowedMethod(HttpMethod.OPTIONS)
            .allowedHeader("Accept")
            .allowedHeader("Authorization")
            .allowedHeader("Content-Type"));

        m_router.get("/health").handler(this::generateHealth);
        m_router.get("/api/resources").handler(this::getAll);
        m_router.get("/api/resources/:id").handler(this::getOne);
        m_router.route("/static/*").handler(StaticHandler.create());
        // set in and outbound permitted addresses
        // Allow events for the designated addresses in/out of the event bus bridge
        BridgeOptions opts = new BridgeOptions()
          .addInboundPermitted(new PermittedOptions().setAddress("client.to.api"))
          .addOutboundPermitted(new PermittedOptions().setAddress("api.to.client"))
          .addOutboundPermitted(new PermittedOptions().setAddress("worker.to.client"));

        // Create the event bus bridge and add it to the router.
        SockJSHandler ebHandler = SockJSHandler.create(vertx).bridge(opts);
        m_router.route("/eventbus/*").handler(ebHandler);

        int port = m_config.getInteger("port", 8080);
        // Create the HTTP server and pass the 
        // "accept" method to the request handler.
        vertx
            .createHttpServer()
            .requestHandler(m_router::accept)
            .listen(
                // Retrieve the port from the 
                // configuration, default to 8080.
                port,
                result -> {
                    if (result.succeeded()) {
                        m_logger.info("Listening now on port {}", port);
                        deployJavaWorker();
                        deployJsWorker();
                        deployKotlinWorker();
                    } else {
                        m_logger.error("Failed to listen", result.cause());
                    }
                }
            );

        // Register to listen for messages coming IN to the server
        vertx.eventBus().consumer("chat.to.server").handler(message -> {
          // Send the message back out to all clients with the timestamp prepended.
          vertx.eventBus().publish("chat.to.client", "Test: ");
        });
    }
    
    private int getNextWorker() {
        int worker_index = 1;
        if (m_current_workers.get() > 0) {
            worker_index = m_next_worker.getAndIncrement();
            if (m_next_worker.get() > m_current_workers.get()) {
                m_next_worker.set(1);
            }
        }
        return worker_index;
    }
    
    private void getAll(RoutingContext rc) {
        rc.response()
            .putHeader("content-type", 
               "application/json; charset=utf-8")
            .end(Json.encodePrettily(null));
        }
    private void getOne(RoutingContext rc) {
        HttpServerResponse response = rc.response();
        String id = rc.request().getParam("id");
        m_logger.info("Request for {} Outstanding {}", id, m_metrics.addPendingRequest());
        int idAsInt = Integer.parseInt(id);
        long startTS = System.nanoTime();
        
        MessageProducer<String> producer = vertx.eventBus().publisher("WORKER" + getNextWorker());
        producer.send(id, result -> {
            if (result.succeeded()) {
                m_logger.info("Sending response for request {} Outstanding {}", id, m_metrics.removePendingRequest());
                m_metrics.incrementCompletedCount();
                m_latency.addAndGet((System.nanoTime() - startTS)/1000000);
                if (response.closed() || response.ended()) {
                    return;
                }
                response
                        .setStatusCode(201)
                        .putHeader("content-type", 
                          "application/json; charset=utf-8")
                        .end(Json.encodePrettily(result.result()));
            } else {
                m_logger.info("Sending error response for request {} Outstanding {}", id, m_metrics.removePendingRequest());
                m_metrics.incrementCompletedCount();
                m_latency.addAndGet((System.nanoTime() - startTS)/1000000);
                if (response.closed() || response.ended()) {
                    return;
                }
                response
                        .setStatusCode(404)
                        .putHeader("content-type", 
                          "application/json; charset=utf-8")
                        .end();
            }
        });
        
    }
    public void generateHealth(RoutingContext ctx) {
        ctx.response()
            .setChunked(true)
            .putHeader("Content-Type", "application/json;charset=UTF-8")
            .putHeader("Access-Control-Allow-Methods", "GET")
            .putHeader("Access-Control-Allow-Origin", "*")
            .putHeader("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type")
            .write((new JsonObject().put("status", "OK")).encode())
            .end();
    }

    private void measure() {
        int qsize = ((ThreadPoolExecutor)m_worker_executor).getQueue().size();
        m_metrics.setWorkerQueueSize(qsize);
        if (m_metrics.getCompletedCount() > 0) {
            m_metrics.setAverageLatency(m_latency.get() / m_metrics.getCompletedCount());
        } else {
            m_metrics.setAverageLatency(0);
        }
        m_metrics.setWorkers(m_current_workers.get());
        
        vertx.eventBus().publish("api.to.client", m_metrics.toString());
        m_metrics.resetCompletedCount();
        m_latency.set(0);
    }
    
//    private void deployWorkers(int count) {
//        if (count > m_current_workers.get()) {
//            while (count > m_current_workers.get()) {
//                addWorker();
//            }
//        } else if (count < m_current_workers.get()) {
//            while (count < m_current_workers.get()) {
//                removeWorker();
//            }
//        }
//    }
    
    private void deployJsWorker() {
        m_current_workers.incrementAndGet();
        JsonObject config = new JsonObject().put("instance", m_current_workers.get());
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setConfig(config)
                .setWorker(true)
                .setInstances(1)
                .setWorkerPoolSize(1)
                .setMultiThreaded(true);
        vertx.deployVerticle("worker_verticle.js", workerOpts, res -> {
            if(res.failed()){
                m_logger.error("Failed to deploy JS worker verticle {}", "worker_verticle.js", res.cause());
            }
            else {
                String depId = res.result();
                m_deployed_verticles.add(depId);
                m_logger.info("Deployed verticle {} DeploymentID {}", "worker_verticle.js", depId);
            }
        });
    }

    private void deployKotlinWorker() {
        m_current_workers.incrementAndGet();
        JsonObject config = new JsonObject().put("instance", m_current_workers.get());
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setConfig(config)
                .setWorker(true)
                .setInstances(1)
                .setWorkerPoolSize(1)
                .setMultiThreaded(true);
        vertx.deployVerticle(KotlinWorkerVerticle.class.getName(), workerOpts, res -> {
            if(res.failed()){
                m_logger.error("Failed to deploy Kotlin worker verticle {}", KotlinWorkerVerticle.class.getName(), res.cause());
            }
            else {
                String depId = res.result();
                m_deployed_verticles.add(depId);
                m_logger.info("Deployed verticle {} DeploymentID {}", KotlinWorkerVerticle.class.getName(), depId);
            }
        });
    }

    private void deployJavaWorker() {
        m_current_workers.incrementAndGet();
        JsonObject config = new JsonObject().put("instance", m_current_workers.get());
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setConfig(config)
                .setWorker(true)
                .setInstances(1)
                .setWorkerPoolSize(1)
                .setMultiThreaded(true);
        vertx.deployVerticle(ApiWorkerVerticle.class.getName(), workerOpts, res -> {
            if(res.failed()){
                m_logger.error("Failed to deploy worker verticle {}", ApiWorkerVerticle.class.getName(), res.cause());
            }
            else {
                String depId = res.result();
                m_deployed_verticles.add(depId);
                m_logger.info("Deployed verticle {} DeploymentID {}", ApiWorkerVerticle.class.getName(), depId);
            }
        });
    }
}