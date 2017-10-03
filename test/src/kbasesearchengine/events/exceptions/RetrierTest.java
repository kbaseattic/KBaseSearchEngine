package kbasesearchengine.events.exceptions;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.events.ObjectStatusEvent;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.Retrier;
import kbasesearchengine.events.exceptions.RetryLogger;
import kbasesearchengine.test.common.TestCommon;

public class RetrierTest {
    
    private class LogEvent {
        private final Instant time;
        private final int retryCount;
        private final Optional<ObjectStatusEvent> event;
        private final RetriableIndexingException exception;
        
        private LogEvent(
                final Instant time,
                final int retryCount,
                final Optional<ObjectStatusEvent> optional,
                final RetriableIndexingException exception) {
            this.time = time;
            this.retryCount = retryCount;
            this.event = optional;
            this.exception = exception;
        }
    }
    
    private class CollectingLogger implements RetryLogger {

        private final List<LogEvent> events = new LinkedList<>();
        
        @Override
        public void log(int retryCount, Optional<ObjectStatusEvent> optional,
                RetriableIndexingException e) {
            events.add(new LogEvent(Instant.now(), retryCount, optional, e));
        }
        
    }

    public void construct() throws Exception {
        final CollectingLogger log = new CollectingLogger();
        final Retrier ret = new Retrier(2, 10, Arrays.asList(4, 5, 6), log);
        
        assertThat("incorrect retries", ret.getRetryCount(), is(2));
        assertThat("incorrect delay", ret.getDelayMS(), is(10));
        assertThat("incorrect fatal delays", ret.getFatalRetryBackoffsMS(),
                is(Arrays.asList(4, 5, 6)));
        assertThat("incorrect logger", ret.getLogger(), is(log));
    }
    
    @Test
    public void constructFail() throws Exception {
        final CollectingLogger log = new CollectingLogger();
        failConstruct(0, 1, Collections.emptyList(), log,
                new IllegalArgumentException("retryCount must be at least 1"));
        failConstruct(1, 0, Collections.emptyList(), log,
                new IllegalArgumentException("delayMS must be at least 1"));
        failConstruct(1, 1, null, log, new NullPointerException("fatalRetryBackoffsMS"));
        failConstruct(1, 1, Arrays.asList(1, null), log,
                new IllegalArgumentException("Illegal value in fatalRetryBackoffsMS: null"));
        failConstruct(1, 1, Arrays.asList(1, 0), log,
                new IllegalArgumentException("Illegal value in fatalRetryBackoffsMS: 0"));
        failConstruct(1, 1, Collections.emptyList(), null, new NullPointerException("logger"));
    }

    private void failConstruct(
            final int retries,
            final int retryDelay,
            final List<Integer> fatalRetryDelay,
            final RetryLogger logger,
            final Exception expected) {
        try {
            new Retrier(retries, retryDelay, fatalRetryDelay, logger);
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void immutableFatalDelayList() {
        final List<Integer> delays = new ArrayList<>();
        delays.add(1);
        delays.add(2);
        delays.add(3);
        final Retrier ret = new Retrier(2, 10, delays, new CollectingLogger());
        try {
            ret.getFatalRetryBackoffsMS().set(2, 42);
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    private class TestConsumer<T> implements RetryConsumer<T> {

        private final T input;
        private final int retries;
        private int count = 0;
        
        private TestConsumer(T input, int retries) {
            this.input = input;
            this.retries = retries;
        }

        @Override
        public void accept(final T t)
                throws IndexingException, RetriableIndexingException, InterruptedException {
            assertThat("incorrect input", t, is(input));
            if (count == retries) {
                return;
            } else {
                count++;
                throw new RetriableIndexingException("bar");
            }
        }
        
    }
    
    @Test
    public void consumer1RetryNoEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(1, 50, Collections.emptyList(), collog);
        final Instant start = Instant.now();
        ret.retryCons(new TestConsumer<>("foo", 1), "foo", null);
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(1));
        final LogEvent le = collog.events.get(0);
        assertThat("incorrect retry count", le.retryCount, is(1));
        assertThat("incorrect event", le.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le.time, 0, 15);
        assertCloseMS(start, end, 50, 15);
    }

    private void assertCloseMS(
            final Instant start,
            final Instant end,
            final int differenceMS,
            final int slopMS) {
        final long gotDiff = end.toEpochMilli() - start.toEpochMilli();
        assertThat(String.format("time difference not within bounds: %s %s %s %s %s",
                start, end, gotDiff, differenceMS, slopMS),
                Math.abs(gotDiff - differenceMS) < slopMS, is(true));
    }

}
