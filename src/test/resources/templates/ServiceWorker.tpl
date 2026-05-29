package com.anjia.unidbgserver.service;

import com.anjia.unidbgserver.config.UnidbgProperties;
import com.github.unidbg.worker.Worker;
import com.github.unidbg.worker.WorkerLoan;
import com.github.unidbg.worker.WorkerPool;
import com.github.unidbg.worker.WorkerPoolFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service("[(${#strings.toLowerCase(#strings.substring(ServiceName,0,1))})][(${#strings.substring(ServiceName,1)})]ServiceWorker")
public class [[${ServiceName}]]ServiceWorker implements Worker {

    private UnidbgProperties unidbgProperties;
    private WorkerPool pool;
    private [[${ServiceName}]]Service [(${#strings.toLowerCase(#strings.substring(ServiceName,0,1))})][(${#strings.substring(ServiceName,1)})]Service;
    @Value("${spring.task.execution.pool.core-size:4}")
    int poolSize;

    public [[${ServiceName}]]ServiceWorker() {
        pool = WorkerPoolFactory.create([[${ServiceName}]]ServiceWorker::new, poolSize);
    }

    public [[${ServiceName}]]ServiceWorker(WorkerPool pool) {
        this.pool = pool;
    }


    @Autowired
    public [[${ServiceName}]]ServiceWorker(UnidbgProperties unidbgProperties) {
        this.unidbgProperties = unidbgProperties;
        if (this.unidbgProperties.isAsync()) {
            pool = WorkerPoolFactory.create(() -> new [[${ServiceName}]]ServiceWorker(unidbgProperties.isDynarmic(),
                unidbgProperties.isVerbose(), pool), Math.max(poolSize, 4));
            log.info("线程池为:{}", poolSize);
        } else {
            this.[(${#strings.toLowerCase(#strings.substring(ServiceName,0,1))})][(${#strings.substring(ServiceName,1)})]Service = new [[${ServiceName}]]Service(unidbgProperties);
        }
    }


    public [[${ServiceName}]]ServiceWorker(boolean dynarmic, boolean verbose, WorkerPool pool) {
        this.pool = pool;
        this.unidbgProperties = new UnidbgProperties();
        unidbgProperties.setDynarmic(dynarmic);
        unidbgProperties.setVerbose(verbose);
        log.info("是否启用动态引擎:{},是否打印详细信息:{}", dynarmic, verbose);
        this.[(${#strings.toLowerCase(#strings.substring(ServiceName,0,1))})][(${#strings.substring(ServiceName,1)})]Service = new [[${ServiceName}]]Service(unidbgProperties);
    }


    @Autowired
    public void init(UnidbgProperties unidbgProperties) {
        this.unidbgProperties = unidbgProperties;
    }

    @Async
    public CompletableFuture<Object> doWork(Object param) {
        [[${ServiceName}]]ServiceWorker worker;
        Object data;
        if (this.unidbgProperties.isAsync()) {
            while (true) {
                try (WorkerLoan<[[${ServiceName}]]ServiceWorker> loan = pool.borrow(2, TimeUnit.SECONDS)) {
                    if (loan == null) {
                        continue;
                    }
                    worker = loan.get();
                    data = worker.exec(param);
                    break;
                }
            }
        } else {
            synchronized (this) {
                data = this.exec(param);
            }
        }
        return CompletableFuture.completedFuture(data);
    }

    @SneakyThrows
    @Override public void destroy() {
        [(${#strings.toLowerCase(#strings.substring(ServiceName,0,1))})][(${#strings.substring(ServiceName,1)})]Service.destroy();
    }

    private Object exec(Object param) {
        return [(${#strings.toLowerCase(#strings.substring(ServiceName,0,1))})][(${#strings.substring(ServiceName,1)})]Service.doWork(param);
    }

}
