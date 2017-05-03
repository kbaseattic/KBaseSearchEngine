package kbaserelationengine.events.reconstructor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.StatusEventListener;
import kbaserelationengine.events.storage.FakeStatusStorage;
import kbaserelationengine.events.storage.MongoDBStatusEventStorage;
import kbaserelationengine.events.storage.StatusEventStorage;
import us.kbase.auth.AuthToken;


public class WSStatusEventReconstructorTest {
	WSStatusEventReconstructorImpl reconstructor;
	StatusEventStorage fakeStorage;
	MongoDBStatusEventStorage mdStorage;
	
	@Before
	public void init() throws MalformedURLException{
        fakeStorage = new FakeStatusStorage();
        mdStorage  = new MongoDBStatusEventStorage("localhost", 27017);

        StatusEventStorage storage = mdStorage;
        
        
        AuthToken token = new AuthToken(System.getenv().get("AUTH_TOKEN"), "unknown") ;
    	URL wsURL = new URL("https://ci.kbase.us/services/ws");
        reconstructor = new WSStatusEventReconstructorImpl(wsURL, token , storage);
        
        // Register listeners
        StatusEventListener listener;         
        listener = new StatusEventListener(){

			@Override
			public void objectStatusChanged(List<ObjectStatusEvent> events) throws IOException {
				for(ObjectStatusEvent obj: events){
					System.out.println(obj);
				}				
			}

			@Override
			public void groupStatusChanged(List<AccessGroupStatus> newStatuses) throws IOException {
				for(AccessGroupStatus obj: newStatuses){
					System.out.println(obj);
				}				
			}

			@Override
			public void groupPermissionsChanged(List<AccessGroupStatus> newStatuses) throws IOException {
				for(AccessGroupStatus obj: newStatuses){
					System.out.println(obj);
				}				
			}
        }; 
//       reconstructor.registerListener(listener);  
        
        reconstructor.registerListener(mdStorage);
	}
	
    @Test
    public void testRefilQueueStorage() throws Exception {
    	Set<Long> excludeWsIds = new HashSet<Long>();
//    	excludeWsIds.add(19971L);
//    	excludeWsIds.add(20281L);
//    	excludeWsIds.add(20266L);
    	
    	System.out.println("Started");
    	reconstructor.processWorkspaceObjects(15L, PresenceType.PRESENT);
    	System.out.println("\tdone - processWorkspaceObjects");
        reconstructor.processWorkspaceObjects(AccessType.PRIVATE, PresenceType.PRESENT, excludeWsIds );
    	System.out.println("\tdone - processWorkspaceObjects");
        reconstructor.processWorkspacePermissions(AccessType.ALL, null);
    	System.out.println("\tdone - processWorkspacePermissions");
        
    }	           	
}
