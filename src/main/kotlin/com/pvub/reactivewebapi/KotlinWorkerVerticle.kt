package com.pvub.reactivewebapi

import org.slf4j.LoggerFactory
import org.slf4j.Logger

/*

  @author Udai
  Created on Oct 7, 2019
*/
class KotlinWorkerVerticle : io.vertx.core.AbstractVerticle()  {
    private var m_logger: Logger = LoggerFactory.getLogger("KOTLIN_WORKER")
    override fun start() 
    {
        m_logger.info("[Kotlin Worker] Starting in ${java.lang.Thread.currentThread().getName()}")

        vertx.eventBus().consumer<String>("KTWORKER", { message ->
          m_logger.info("[Kotlin Worker] Consuming data in ${java.lang.Thread.currentThread().getName()}")
          var body = message.body()
          message.reply(body + "-reply")
        })
    }
}