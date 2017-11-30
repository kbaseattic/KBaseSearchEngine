package kbasesearchengine.events.storage;

/** 
 * Thrown when an exception occurs regarding initialization of the event storage system
 * @author gaprice@lbl.gov
 *
 */
public class StorageInitException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public StorageInitException(String message) { super(message); }
	public StorageInitException(String message, Throwable cause) {
		super(message, cause);
	}
}
