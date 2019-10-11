package com.pvub.reactivewebapi;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.rxjava.core.Future;
import java.util.ArrayList;

/**
 * A Load Balancer for handling Asynchronous message passing
 * using Worker Verticles
 * @author Udai
 */
public class AsyncRequestLB {
    private Vertx           m_vertx;
    private JsonObject      m_config;
    private Integer         m_worker_instances;
    private Integer         m_next_worker = 0;
    private Integer         m_max_workers;
    private ArrayList<MessageProducer<String>> m_producers;
    
    public AsyncRequestLB(JsonObject config) {
        m_config            = config;
        m_max_workers       = m_config.getInteger("max-workers", 100);
        m_worker_instances  = m_config.getInteger("workers", 10);
        m_vertx             = Vertx.currentContext().owner();
        createProducers();
    }
    
    private void createProducers() {
        for (short count = 1; count <= m_max_workers; ++count) {
            m_producers.add(m_vertx.eventBus().publisher("WORKER"+count));
        }
    }
    
    public Future<Resource> getResource(String id) {
        Future<Resource> f = Future.future();
        m_producers.get(m_next_worker++).send(id, ar -> {
            if (ar.succeeded()) {
                String json_response = (String) ar.result().body();
            } else {
                f.fail("Failed to obtain resource");
            }
        });
        return f;
    }
}
