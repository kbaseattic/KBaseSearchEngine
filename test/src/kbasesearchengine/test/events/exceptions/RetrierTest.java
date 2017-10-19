package kbasesearchengine.test.events.exceptions;

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

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventID;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.StatusEventWithID;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.exceptions.Retrier;
import kbasesearchengine.events.exceptions.RetriesExceededIndexingException;
import kbasesearchengine.events.exceptions.RetryConsumer;
import kbasesearchengine.events.exceptions.RetryFunction;
import kbasesearchengine.events.exceptions.RetryLogger;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;

public class RetrierTest {
    
    private class LogEvent {
        private final Instant time;
        private final int retryCount;
        private final Optional<StatusEventWithID> event;
        private final RetriableIndexingException exception;
        
        private LogEvent(
                final Instant time,
                final int retryCount,
                final Optional<StatusEventWithID> optional,
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
        public void log(int retryCount, Optional<StatusEventWithID> optional,
                RetriableIndexingException e) {
            events.add(new LogEvent(Instant.now(), retryCount, optional, e));
        }
        
    }

    @Test
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
        private final boolean fatal;
        
        private TestConsumer(T input, int retries) {
            this.input = input;
            this.retries = retries;
            fatal = false;
        }
        
        private TestConsumer(T input, int retries, boolean fatal) {
            this.input = input;
            this.retries = retries;
            this.fatal = fatal;
        }

        @Override
        public void accept(final T t)
                throws IndexingException, RetriableIndexingException, InterruptedException {
            assertThat("incorrect input", t, is(input));
            if (count == retries) {
                return;
            } else {
                count++;
                if (fatal) {
                    throw new FatalRetriableIndexingException("game over man");
                } else {
                    throw new RetriableIndexingException("bar");
                }
            }
        }
        
    }
    
    @Test
    public void consumer1RetrySuccessNoEvent() throws Exception {
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
    
    @Test
    public void consumer2RetrySuccessWithEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Collections.emptyList(), collog);
        final StatusEventWithID ev = new StatusEventWithID(StatusEvent.getBuilder(
                new StorageObjectType("foo", "whee"), Instant.now(), StatusEventType.DELETED)
                .withNullableAccessGroupID(23)
                .withNullableObjectID("bar")
                .withNullableVersion(6)
                .build(),
                new StatusEventID("wugga"));
        final Instant start = Instant.now();
        ret.retryCons(new TestConsumer<>("foo", 2), "foo", ev);
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le1.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le2.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le2.time, 50, 15);
        assertCloseMS(start, end, 100, 15);
    }
    
    @Test
    public void consumerRetriesExceeded() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Collections.emptyList(), collog);
        final Instant start = Instant.now();
        try {
            ret.retryCons(new TestConsumer<>("foo", -1), "foo", null);
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new RetriesExceededIndexingException("bar"));
        }
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le1.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le2.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le2.time, 50, 15);
        assertCloseMS(start, end, 100, 15);
    }
    
    @Test
    public void consumer1FatalRetrySuccessNoEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(1, 50, Arrays.asList(70), collog);
        final Instant start = Instant.now();
        ret.retryCons(new TestConsumer<>("foo", 1, true), "foo", null);
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(1));
        final LogEvent le = collog.events.get(0);
        assertThat("incorrect retry count", le.retryCount, is(1));
        assertThat("incorrect event", le.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le.time, 0, 15);
        assertCloseMS(start, end, 70, 15);
    }
    
    @Test
    public void consumer2FatalRetrySuccessWithEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Arrays.asList(70, 30), collog);
        final StatusEventWithID ev = new StatusEventWithID(StatusEvent.getBuilder(
                new StorageObjectType("foo", "whee"), Instant.now(), StatusEventType.DELETED)
                .withNullableAccessGroupID(23)
                .withNullableObjectID("bar")
                .withNullableVersion(6)
                .build(),
                new StatusEventID("wugga"));
        final Instant start = Instant.now();
        ret.retryCons(new TestConsumer<>("foo", 2, true), "foo", ev);
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le1.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le2.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le2.time, 70, 15);
        assertCloseMS(start, end, 100, 15);
    }
    
    @Test
    public void consumerFatalRetriesExceeded() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Arrays.asList(30, 70), collog);
        final Instant start = Instant.now();
        try {
            ret.retryCons(new TestConsumer<>("foo", -1, true), "foo", null);
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new FatalIndexingException("game over man"));
        }
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le1.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le2.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le2.time, 30, 15);
        assertCloseMS(start, end, 100, 15);
    }

    private class TestFunction<T, R> implements RetryFunction<T, R> {

        private final T input;
        private final R ret;
        private final int retries;
        private int count = 0;
        private final boolean fatal;
        
        private TestFunction(T input, R ret, int retries) {
            this.input = input;
            this.ret = ret;
            this.retries = retries;
            fatal = false;
        }
        
        private TestFunction(T input, R ret, int retries, final boolean fatal) {
            this.input = input;
            this.ret = ret;
            this.retries = retries;
            this.fatal = fatal;
        }

        @Override
        public R apply(final T t)
                throws IndexingException, RetriableIndexingException, InterruptedException {
            assertThat("incorrect input", t, is(input));
            if (count == retries) {
                return ret;
            } else {
                count++;
                if (fatal) {
                    throw new FatalRetriableIndexingException("game over man");
                } else {
                    throw new RetriableIndexingException("bar");
                }
            }
        }
    }

    @Test
    public void function1RetrySuccessNoEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(1, 50, Collections.emptyList(), collog);
        final Instant start = Instant.now();
        final long result = ret.retryFunc(new TestFunction<>("foo", 24L, 1), "foo", null);
        final Instant end = Instant.now();
        
        assertThat("incorrect result", result, is(24L));
        assertThat("incorrect retries", collog.events.size(), is(1));
        final LogEvent le = collog.events.get(0);
        assertThat("incorrect retry count", le.retryCount, is(1));
        assertThat("incorrect event", le.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le.time, 0, 15);
        assertCloseMS(start, end, 50, 15);
    }
    
    @Test
    public void function2RetrySuccessWithEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Collections.emptyList(), collog);
        final StatusEventWithID ev = new StatusEventWithID(StatusEvent.getBuilder(
                new StorageObjectType("foo", "whee"), Instant.now(), StatusEventType.DELETED)
                .withNullableAccessGroupID(23)
                .withNullableObjectID("bar")
                .withNullableVersion(6)
                .build(),
                new StatusEventID("wugga"));
        final Instant start = Instant.now();
        final long result = ret.retryFunc(new TestFunction<>("foo", 26L, 2), "foo", ev);
        final Instant end = Instant.now();
        
        assertThat("incorrect result", result, is(26L));
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le1.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le2.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le2.time, 50, 15);
        assertCloseMS(start, end, 100, 15);
    }
    
    @Test
    public void functionRetriesExceeded() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Collections.emptyList(), collog);
        final Instant start = Instant.now();
        try {
            ret.retryFunc(new TestFunction<>("foo", 3L, -1), "foo", null);
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new RetriesExceededIndexingException("bar"));
        }
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le1.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le2.exception, new RetriableIndexingException("bar"));
        assertCloseMS(start, le2.time, 50, 15);
        assertCloseMS(start, end, 100, 15);
    }
    
    @Test
    public void function1FatalRetrySuccessNoEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(1, 50, Arrays.asList(70), collog);
        final Instant start = Instant.now();
        final long result = ret.retryFunc(new TestFunction<>("foo", 42L, 1, true), "foo", null);
        final Instant end = Instant.now();
        
        assertThat("incorrect result", result, is(42L));
        assertThat("incorrect retries", collog.events.size(), is(1));
        final LogEvent le = collog.events.get(0);
        assertThat("incorrect retry count", le.retryCount, is(1));
        assertThat("incorrect event", le.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le.time, 0, 15);
        assertCloseMS(start, end, 70, 15);
    }
    
    @Test
    public void function2FatalRetrySuccessWithEvent() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Arrays.asList(70, 30), collog);
        final StatusEventWithID ev = new StatusEventWithID(StatusEvent.getBuilder(
                new StorageObjectType("foo", "whee"), Instant.now(), StatusEventType.DELETED)
                .withNullableAccessGroupID(23)
                .withNullableObjectID("bar")
                .withNullableVersion(6)
                .build(),
                new StatusEventID("wugga"));
        final Instant start = Instant.now();
        final long result = ret.retryFunc(new TestFunction<>("foo", 64L, 2, true), "foo", ev);
        final Instant end = Instant.now();
        
        assertThat("incorrect result", result, is(64L));
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le1.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.of(ev)));
        TestCommon.assertExceptionCorrect(le2.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le2.time, 70, 15);
        assertCloseMS(start, end, 100, 15);
    }
    
    @Test
    public void functionFatalRetriesExceeded() throws Exception {
        final CollectingLogger collog = new CollectingLogger();
        final Retrier ret = new Retrier(2, 50, Arrays.asList(30, 70), collog);
        final Instant start = Instant.now();
        try {
            ret.retryFunc(new TestFunction<>("foo", 43L, -1, true), "foo", null);
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new FatalIndexingException("game over man"));
        }
        final Instant end = Instant.now();
        
        assertThat("incorrect retries", collog.events.size(), is(2));
        final LogEvent le1 = collog.events.get(0);
        assertThat("incorrect retry count", le1.retryCount, is(1));
        assertThat("incorrect event", le1.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le1.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le1.time, 0, 15);
        
        final LogEvent le2 = collog.events.get(1);
        assertThat("incorrect retry count", le2.retryCount, is(2));
        assertThat("incorrect event", le2.event, is(Optional.absent()));
        TestCommon.assertExceptionCorrect(le2.exception,
                new FatalRetriableIndexingException("game over man"));
        assertCloseMS(start, le2.time, 30, 15);
        assertCloseMS(start, end, 100, 15);
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
