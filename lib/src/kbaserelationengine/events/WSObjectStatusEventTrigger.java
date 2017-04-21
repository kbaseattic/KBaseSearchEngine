package kbaserelationengine.events;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WSObjectStatusEventTrigger implements ObjectStatusEventTrigger{
	private List<ObjectStatusEventListener> eventListeners = new ArrayList<ObjectStatusEventListener>(); 

	@Override
	public void registerListener(ObjectStatusEventListener listener) {
		eventListeners.add(listener);
	}

	@Override
	public void trigger(ObjectStatusEvent event) throws IOException {
		for(ObjectStatusEventListener listner: eventListeners){
			listner.statusChanged(event);
		}
		
	}
	
	

}
