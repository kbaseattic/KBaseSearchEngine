package kbasesearchengine.events.reconstructor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;

import kbasesearchengine.events.AccessGroupStatus;
import kbasesearchengine.events.ObjectStatusEvent;
import kbasesearchengine.events.StatusEventListener;
import kbasesearchengine.events.reconstructor.AccessType;
import kbasesearchengine.events.reconstructor.PresenceType;
import kbasesearchengine.events.reconstructor.WSStatusEventReconstructorImpl;
import kbasesearchengine.events.storage.FakeStatusStorage;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.events.storage.StatusEventStorage;
import us.kbase.auth.AuthToken;


public class WSStatusEventReconstructorTest {
	WSStatusEventReconstructorImpl reconstructor;
	StatusEventStorage fakeStorage;
	MongoDBStatusEventStorage mdStorage;
	
	@Before
	public void init() throws MalformedURLException{
        fakeStorage = new FakeStatusStorage();
        @SuppressWarnings("resource")
        final MongoClient mongoClient = new MongoClient("localhost");
        mdStorage  = new MongoDBStatusEventStorage(mongoClient
                .getDatabase("reske_test_db"));

        StatusEventStorage storage = mdStorage;
        
        
        AuthToken token = new AuthToken(System.getenv().get("AUTH_TOKEN"), "unknown") ;
    	URL wsURL = new URL("https://ci.kbase.us/services/ws");
        reconstructor = new WSStatusEventReconstructorImpl(wsURL, token , storage);
        
        // Register listeners
        @SuppressWarnings("unused")
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
