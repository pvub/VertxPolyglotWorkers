console.log("[Worker] Starting in " + Java.type("java.lang.Thread").currentThread().getName());

console.log("Configuration instance=" + Vertx.currentContext().config().instance);
var instance = Vertx.currentContext().config().instance;

var perf_metrics = {
    instance: instance,
    queue: 0,
    completed: 0,
    avg_lat: 0
}

var completed = 0;

vertx.eventBus().consumer("WORKER"+instance, function (message) {
  console.log("[Worker] Consuming data in " + Java.type("java.lang.Thread").currentThread().getName());
  var identifier = message.body();
  var r = {
      id: identifier,
      title: "",
      url: ""
  }
  message.reply(JSON.stringify(r));
  ++completed;
});

vertx.setPeriodic(1000, function (id) {
    perf_metrics.completed = completed;
    completed = 0;
    vertx.eventBus().publish("api.to.client", JSON.stringify(perf_metrics));
});
