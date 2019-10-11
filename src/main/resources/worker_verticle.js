console.log("[Worker] Starting in " + Java.type("java.lang.Thread").currentThread().getName());

vertx.eventBus().consumer("js-worker", function (message) {
  console.log("[Worker] Consuming data in " + Java.type("java.lang.Thread").currentThread().getName());
  var body = message.body();
  message.reply(body.toUpperCase());
});

