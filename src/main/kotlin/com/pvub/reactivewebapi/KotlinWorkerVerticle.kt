package com.pvub.reactivewebapi

import io.vertx.core.json.JsonObject
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import com.pvub.reactivewebapi.Resource
import io.vertx.core.json.Json;
import rx.Observable
import rx.Subscription

/*

  @author Udai
  Created on Oct 7, 2019
*/
class KotlinWorkerVerticle : io.vertx.core.AbstractVerticle()  {
    private var m_instance = 0
    private var m_completed = 0
    private var m_logger: Logger = LoggerFactory.getLogger("KOTLIN_WORKER")
    private var m_metrics_obj: JsonObject = JsonObject()
    override fun start() 
    {
        m_logger.info("[Kotlin Worker] Starting in ${java.lang.Thread.currentThread().getName()}")
        m_instance = config().getInteger("instance")
        m_logger.info("[Kotlin Worker] instance ${m_instance}")

        vertx.setPeriodic(1000, { id ->
            m_metrics_obj.put("instance", m_instance)
            m_metrics_obj.put("queue", 0)
            m_metrics_obj.put("completed", m_completed)
            m_metrics_obj.put("avg_lat", 0)
            m_completed = 0
            vertx.eventBus().publish("api.to.client", Json.encode(m_metrics_obj));
        })
        
        vertx.eventBus().consumer<String>("WORKER"+m_instance, { message ->
          m_logger.info("[Kotlin Worker] Consuming data in ${java.lang.Thread.currentThread().getName()}")
          var id = message.body().toInt()
          var r = Resource(id, "", "")
          message.reply(Json.encodePrettily(r))
          ++m_completed
        })
    }
    override fun stop() {
        m_logger.info("[Kotlin Worker] Stopping in ${java.lang.Thread.currentThread().getName()}")
    }
}