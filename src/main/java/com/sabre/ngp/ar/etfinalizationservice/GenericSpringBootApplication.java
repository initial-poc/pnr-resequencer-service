package com.sabre.ngp.ar.etfinalizationservice;

import com.sabre.ngp.ar.etfinalizationservice.component.OutboxPollerExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class GenericSpringBootApplication implements CommandLineRunner {

    @Autowired
    private OutboxPollerExecutor executor;
    public static void main(String[] args) throws Exception{
  // SpringApplication.run(GenericSpringBootApplication.class, args);
        {
            ThreadPoolExecutor threadPoolExecutor =   new ThreadPoolExecutor(5, 5,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(10));
            threadPoolExecutor.submit(() -> {
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            System.out.println(threadPoolExecutor.getActiveCount());
        }
    }

    @Override
    public void run(String... args) throws Exception {
        executor.process();
    }
}
