package kbasesearchengine.events.reconstructor;

import java.io.IOException;
import java.util.Set;

public interface WSStatusEventReconstructor {
			
	public void processWorkspaceObjects(
			AccessType wsAccessType, 
			PresenceType objPresenceType,
			Set<Long> excludedWsIds) throws IOException;
	
	public void processWorkspaceObjects( Long wsId, 
			PresenceType objPresenceType) throws IOException;
	
	public void processWorkspacePermissions(
			AccessType wsAccessType, 
			Set<Long> excludedWsIds) throws IOException;
		
	
//	public void processWorkspaceObjects(List<Long> workspaceIds, 
//			PresenceType objPresenceType, ObjectType objType) throws IOException;
//
		
}
