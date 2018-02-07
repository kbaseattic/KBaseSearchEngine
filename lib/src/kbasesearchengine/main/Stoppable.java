package kbasesearchengine.main;

/** An interface for a service or task that can be stopped.
 * @author gaprice@lbl.gov
 *
 */
public interface Stoppable {
    
    /** Stop the Stoppable.
     * @param millisToWait the number of milliseconds to wait for the service or task to complete
     * before the task is stopped.
     * @throws InterruptedException if the thread is interrupted while waiting for the service
     * or task to stop.
     */
    void stop(long millisToWait) throws InterruptedException;
    
    /** Wait for the {@link Stoppable} to shut down of its own volition. 
     * @throws InterruptedException if the thread is interrupted.
     */
    void awaitShutdown() throws InterruptedException;

}
