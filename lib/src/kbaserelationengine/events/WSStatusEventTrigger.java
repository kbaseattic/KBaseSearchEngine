package kbaserelationengine.events;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WSStatusEventTrigger implements StatusEventTrigger{
	private List<StatusEventListener> eventListeners = new ArrayList<StatusEventListener>(); 

	@Override
	public void registerListener(StatusEventListener listener) {
		eventListeners.add(listener);
	}

	@Override
	public void trigger(ObjectStatusEvent event) throws IOException {
		for(StatusEventListener listner: eventListeners){
			listner.statusChanged(event);
		}		
	}
	
	@Override
	public void trigger(AccessGroupStatus newStatus) throws IOException {
		for(StatusEventListener listner: eventListeners){
			listner.statusChanged(newStatus);
		}		
	}
	

}
