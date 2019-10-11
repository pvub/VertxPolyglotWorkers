package com.pvub.reactivewebapi;

import io.vertx.rxjava.core.Future;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
import rx.schedulers.Schedulers;

/**
 * A simple opaque representation of resource provider
 * @author Udai
 */
public class ResourceAPI {
    private ConcurrentHashMap<Integer, Resource> m_resources;
    private Random                               s_rand;
    private final Logger                         m_logger;
    private Integer                              m_max_delay_milliseconds;
    private final Scheduler                      m_scheduler;
    private AtomicLong                           m_async_requests = new AtomicLong(0);
    public ResourceAPI(final ExecutorService executor, Integer max_delay_milliseconds) {
        m_logger = LoggerFactory.getLogger("API");
        m_max_delay_milliseconds = max_delay_milliseconds;
        m_scheduler = Schedulers.from(executor);
    }
    
    public void build() {
        m_resources = new ConcurrentHashMap<Integer, Resource>();
        for (int i=1; i<=100; ++i) {
            m_resources.put(i, new Resource(i));
        }
        s_rand = new Random();
        s_rand.setSeed(System.currentTimeMillis());
    }
    public Future<Resource> getResource(int id) {
        Future<Resource> f = Future.future();
        int random_interval = s_rand.nextInt(m_max_delay_milliseconds);
        m_logger.info("Request for {} will execute in {} milliseconds max {} milliseconds", id, random_interval, m_max_delay_milliseconds);
        Observable.timer(random_interval, TimeUnit.MILLISECONDS)
                  .observeOn(m_scheduler)
                  .subscribe(delay -> {
                            m_logger.info("Returning resource for {}", id);
                            Resource r = m_resources.get(id);
                            f.complete(r);
                        }, 
                        error -> {}, () -> {});
        return f;
    }
    
    public Observable<Resource> fetchResource(int id) {
        return Observable.create(subscriber -> {
            m_async_requests.incrementAndGet();
            int random_interval = s_rand.nextInt(m_max_delay_milliseconds);
            m_logger.info("Request for {} will execute in {} milliseconds max {} milliseconds", id, random_interval, m_max_delay_milliseconds);
            Observable.timer(random_interval, TimeUnit.MILLISECONDS)
                      .observeOn(m_scheduler)
                      .subscribe(delay -> {
                                m_async_requests.decrementAndGet();
                                m_logger.info("Returning resource for {}", id);
                                Resource r = m_resources.get(id);
                                subscriber.onNext(r);
                                subscriber.onCompleted();
                            }, 
                            error -> {
                                subscriber.onError(error);
                            }, 
                            () -> {});
        });
    }
    public long getAsyncRequests() {
        return m_async_requests.get();
    }
}
