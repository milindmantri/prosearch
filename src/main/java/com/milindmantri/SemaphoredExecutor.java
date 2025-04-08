package com.milindmantri;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public class SemaphoredExecutor {
  private final Semaphore sem;
  private final ExecutorService exec;

  public SemaphoredExecutor(final ExecutorService exec, final int permits) {
    this.exec = exec;
    this.sem = new Semaphore(permits);
  }

  public Future<?> submit(final Callable<?> c) {

    return this.exec.submit(
        () -> {
          try {
            this.sem.acquire();
            var ret = c.call();
            Thread.sleep(Duration.ofSeconds(2));
            return ret;
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } finally {
            this.sem.release();
          }
        });
  }
}
