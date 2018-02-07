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
    
    private final CountDownLatch latch = new CountDownLatch(1);
    
    public SignalMonitor() {}
    
    /** Send a signal to the waiting thread. */
    public void signal() {
        latch.countDown();
    }

    
    /** Wait for a signal to be sent by the controlling thread.
     * @throws InterruptedException if the thread is interrupted.
     */
    public void awaitSignal() throws InterruptedException {
        latch.await();
    }

}
