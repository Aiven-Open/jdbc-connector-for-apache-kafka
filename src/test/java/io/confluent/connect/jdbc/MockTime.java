package io.confluent.connect.jdbc;

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.TimeUnit;

/**
 * A clock that you can manually advance by calling sleep
 */
public class MockTime implements Time {

    private long nanos = 0;
    private long autoTickMs = 0;

    public MockTime() {
        this.nanos = System.nanoTime();
    }

    public MockTime(long autoTickMs) {
        this.nanos = System.nanoTime();
        this.autoTickMs = autoTickMs;
    }

    @Override
    public long milliseconds() {
        this.sleep(autoTickMs);
        return TimeUnit.MILLISECONDS.convert(this.nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long nanoseconds() {
        this.sleep(autoTickMs);
        return nanos;
    }

    @Override
    public void sleep(long ms) {
        this.nanos += TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
    }

}
