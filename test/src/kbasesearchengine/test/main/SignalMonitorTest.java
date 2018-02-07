package kbasesearchengine.test.main;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import kbasesearchengine.main.SignalMonitor;
import kbasesearchengine.test.common.TestCommon;

public class SignalMonitorTest {
    
    @Test
    public void signal() throws Exception {
        final SignalMonitor sm = new SignalMonitor();
        
        final Thread signalThread = new Thread() {
            
            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Test borked", e);
                }
                sm.signal();
            }
        };
        
        final Instant now = Instant.now();
        signalThread.start();
        sm.awaitSignal();
        TestCommon.assertCloseMS(now, Instant.now(), 200, 50);
    }

}
