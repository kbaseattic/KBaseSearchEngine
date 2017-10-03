package kbasesearchengine.events.exceptions;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
        private final long timeMS;
        private final int retryCount;
        private final Optional<ObjectStatusEvent> optional;
        private final RetriableIndexingException exception;
        
        private LogEvent(
                final long timeMS,
                final int retryCount,
                final Optional<ObjectStatusEvent> optional,
                final RetriableIndexingException exception) {
            this.timeMS = timeMS;
            this.retryCount = retryCount;
            this.optional = optional;
            this.exception = exception;
        }
    }
    
    private class CollectingLogger implements RetryLogger {

        private final List<LogEvent> events = new LinkedList<>();
        
        @Override
        public void log(int retryCount, Optional<ObjectStatusEvent> optional,
                RetriableIndexingException e) {
            events.add(new LogEvent(System.currentTimeMillis(), retryCount, optional, e));
        }
        
    }
    
    private static final RetryLogger LOG = new RetryLogger() {
        
        @Override
        public void log(
                final int retryCount,
                final Optional<ObjectStatusEvent> optional,
                final RetriableIndexingException e) {
            // do nothing
        }
    };
    
    @Test
    public void construct() throws Exception {
        final Retrier ret = new Retrier(2, 10, Arrays.asList(4, 5, 6), LOG);
        
        assertThat("incorrect retries", ret.getRetryCount(), is(2));
        assertThat("incorrect delay", ret.getDelayMS(), is(10));
        assertThat("incorrect fatal delays", ret.getFatalRetryBackoffsMS(),
                is(Arrays.asList(4, 5, 6)));
        assertThat("incorrect logger", ret.getLogger(), is(LOG));
    }
    
    @Test
    public void constructFail() throws Exception {
        failConstruct(0, 1, Collections.emptyList(), LOG,
                new IllegalArgumentException("retryCount must be at least 1"));
        failConstruct(1, 0, Collections.emptyList(), LOG,
                new IllegalArgumentException("delayMS must be at least 1"));
        failConstruct(1, 1, null, LOG, new NullPointerException("fatalRetryBackoffsMS"));
        failConstruct(1, 1, Arrays.asList(1, null), LOG,
                new IllegalArgumentException("Illegal value in fatalRetryBackoffsMS: null"));
        failConstruct(1, 1, Arrays.asList(1, 0), LOG,
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
        final Retrier ret = new Retrier(2, 10, delays, LOG);
        try {
            ret.getFatalRetryBackoffsMS().set(2, 42);
        } catch (UnsupportedOperationException e) {
            // test passed
        }
    }
    
    @Test
    public void consumer1Retry() {
        final CollectingLogger collog = new CollectingLogger();
    }

}
