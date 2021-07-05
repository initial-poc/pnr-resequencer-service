package com.sabre.ngp.ar.etfinalizationservice.component;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class OutboxPollerExecutorTests {

    @Autowired
    private OutboxPollerExecutor outboxPollerExecutor;

    @Test
    public void test_basic_flow() {
        outboxPollerExecutor.process();
    }
}
