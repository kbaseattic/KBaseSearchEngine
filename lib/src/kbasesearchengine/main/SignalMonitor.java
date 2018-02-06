package kbasesearchengine.main;

import java.util.concurrent.CountDownLatch;

/** Class for a thread to wait until another thread sends a signal.
 * 
 * {@link #awaitSignal()} should be made accessible, even indirectly, to the waiting thread.
 * 
 * The waiting thread then calls {@link #awaitSignal()}. The waiting thread will then lie
 * dormant until the signaling thread calls {@link #signal()}.
 * 
 * Only one signal can be sent - to send another signal, create a new class.
 * 
 * @author gaprice@lbl.gov
 *
 */
public class SignalMonitor {
    
    final CountDownLatch latch = new CountDownLatch(1);
    
    public SignalMonitor() {}
    
    public void signal() {
        latch.countDown();
    }
    
    public void awaitSignal() throws InterruptedException {
        latch.await();
    }

}
