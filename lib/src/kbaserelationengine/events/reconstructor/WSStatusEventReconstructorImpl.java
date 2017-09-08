package kbaserelationengine.events.reconstructor;

/**
 * TODO: process undeleted Workspaces: in the current implementation it may happen that 
 * the object CREATE event will be duplicated for such workspaces
 */

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.events.StatusEventListener;
import kbaserelationengine.events.StatusEventTrigger;
import kbaserelationengine.events.storage.StatusEventStorage;
import kbaserelationengine.system.StorageObjectType;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.UnauthorizedException;
import workspace.GetObjects2Params;
import workspace.GetObjects2Results;
import workspace.ListObjectsParams;
import workspace.ListWorkspaceInfoParams;
import workspace.ObjectData;
import workspace.ObjectSpecification;
import workspace.WorkspaceClient;
import workspace.WorkspaceIdentity;

public class WSStatusEventReconstructorImpl implements WSStatusEventReconstructor, StatusEventTrigger{
	
	private static String WS_STORAGE_CODE = "WS";
	private static String DATA_PALETTE_TYPE = "DataPalette.DataPalette";
	
	private URL wsUrl;
	private AuthToken token;
	private StatusEventStorage objStatusStorage;
	
	private WorkspaceClient _wsClient;
	private List<ObjectStatusEvent> objectStatusEventsBuffer = new ArrayList<ObjectStatusEvent>();
	private List<ObjectDescriptor> objectDescriptorsBuffer = new ArrayList<ObjectDescriptor>();
	private List<AccessGroupStatus> accessGroupStatusBuffer = new ArrayList<AccessGroupStatus>();
	private List<StatusEventListener> eventListeners = new ArrayList<StatusEventListener>(); 
		
	public WSStatusEventReconstructorImpl(URL wsUrl, AuthToken token, 
			StatusEventStorage objStatusStorage) {
		this.wsUrl = wsUrl;
		this.token = token;
		this.objStatusStorage = objStatusStorage;
	}
		
	@Override
	public void registerListener(StatusEventListener listener) {
		eventListeners.add(listener);
	}
	
	@Override
	public void processWorkspaceObjects(
			AccessType wsAccessType, 
			PresenceType objPresenceType,
			Set<Long> excludedWsIds) throws IOException{
		
		// Get current state: workspaces status  
		try {
	    	List<WorkspaceDescriptor> wss = getWorkspacesDescriptors(wsAccessType, objPresenceType, excludedWsIds);
	    	Collections.sort(wss);
	    	
	    	processWorkspaceObjects(wss, objPresenceType);
	    	
		} catch (JsonClientException e) {
			throw new IOException(e);
		}						
	}

	@Override
	public void processWorkspaceObjects(Long wsId, PresenceType objPresenceType) throws IOException {
		
		// Get current state: workspaces status  
		try {
			WorkspaceDescriptor ws = getWorkspacesDescriptor(wsId);	    	
	    	processWorkspaceObjects(Arrays.asList(ws), objPresenceType);	    	
		} catch (JsonClientException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void processWorkspacePermissions(
			AccessType wsAccessType, 
			Set<Long> excludedWsIds) throws IOException{
		
		try {
			// We will process only PRESENT workspaces
	    	List<WorkspaceDescriptor> wss = getWorkspacesDescriptors(wsAccessType, PresenceType.PRESENT, excludedWsIds);
	    	attachPreviousState(wss);
	    	updateWorskapcesPermissions(wss);	    				
		} catch (JsonClientException e) {
			throw new IOException(e);
		}		
	};	
	
	private void processWorkspaceObjects(List<WorkspaceDescriptor> wss, 
			PresenceType objPresenceType) throws IOException{
		try {
			// Get history: accessGroups (workspaces) processed before
			List<AccessGroupStatus> ags = objStatusStorage.findAccessGroups(WS_STORAGE_CODE);
			Collections.sort(ags);
				    	
	    	// Filter by timestamps
	    	List<WorkspaceDescriptor> filteredWss = filterByTimestamps(wss, ags);	    	
	    	
	    	// Phase 1: do all objects

	    	// First do present objects
	    	if(objPresenceType == PresenceType.PRESENT || objPresenceType == PresenceType.ALL){
		    	for(WorkspaceDescriptor ws: filteredWss){
		    		if(!ws.isDeleted) {
		    			doWorkspacePresentObjects(ws);
		    		}
		    	}
	    	}
	    	
	    	// Then do deleted objects
	    	if(objPresenceType == PresenceType.DELETED || objPresenceType == PresenceType.ALL){
		    	for(WorkspaceDescriptor ws: filteredWss){
		    		if(ws.isDeleted) {
		    			doDeletedWorkspace(ws);
		    		} else{
			    		doWorkspaceDeletedObjects(ws); 		    		
		    		}		    		
		    	}	    			    		
	    	}
	    	
	    	// Phase 2: do all data palettes 
	    	
	    	// First do present data palettes
	    	if(objPresenceType == PresenceType.PRESENT || objPresenceType == PresenceType.ALL){
	    		for(WorkspaceDescriptor ws: filteredWss){
		    		if(!ws.isDeleted) {
		    			doWorkspaceDataPalettes(ws);
		    		}
	    		}
	    	}
	    	
	    	// Then do deleted data palettes
	    	if(objPresenceType == PresenceType.DELETED || objPresenceType == PresenceType.ALL){
	    		// Do not do anything. Data Palette object can not be deleted, 
	    		// unless the whole workspace is deleted, and it was processed already in the 
	    		// doDeletedWorkspace method
	    	}
	    	
    		// Phase 3: update all workspace states
	    	updateWorskapcesState(filteredWss);
	    	
		} catch (JsonClientException e) {
			throw new IOException(e);
		}				
	};
	
	
	private void updateWorskapcesPermissions(List<WorkspaceDescriptor> wss) throws IOException, JsonClientException {
		accessGroupStatusBuffer.clear();
		processWorkspaceStates(wss,accessGroupStatusBuffer, true);
		groupPermissionsChanged(accessGroupStatusBuffer);							
	}

	private void updateWorskapcesState(List<WorkspaceDescriptor> wss) throws IOException, JsonClientException {
		accessGroupStatusBuffer.clear();
		processWorkspaceStates(wss,accessGroupStatusBuffer, false);
		groupStatusChanged(accessGroupStatusBuffer);					
	}	
		
	private void attachPreviousState(List<WorkspaceDescriptor> wss) throws IOException {
		
		// Get info about workspaces processed previously
		List<AccessGroupStatus> ags = objStatusStorage.findAccessGroups(WS_STORAGE_CODE);
		Map<Integer, AccessGroupStatus> wsId2ags = new Hashtable<Integer, AccessGroupStatus>(); 
		for(AccessGroupStatus ag: ags){
			wsId2ags.put(ag.getAccessGroupId(), ag);			
		}
		
		// attach 
		for(WorkspaceDescriptor ws : wss){
			ws.ag = wsId2ags.get(ws.wsId);
		}		
	}

	private List<WorkspaceDescriptor> filterByTimestamps(List<WorkspaceDescriptor> wss, List<AccessGroupStatus> ags) {
		
		List<WorkspaceDescriptor> filteredWss = new ArrayList<WorkspaceDescriptor>();
		
		// Both lists are expected to be sorted, but will sort them just ofr the case
		Collections.sort(wss);
		Collections.sort(ags);
				
    	int wssIndex = 0;
    	int agsIndex = 0;
    	  	
    	while(true){
    		
    		if(wssIndex >= wss.size() ) break;
    		if(agsIndex >= ags.size()){
    			// it means that there are no workspaces in the history (processed before)
    			// just process the rest of workspaces from wss
    			for(; wssIndex < wss.size(); wssIndex ++){
    				WorkspaceDescriptor _ws = wss.get(wssIndex);
    				_ws.ag = null;
    				filteredWss.add(_ws);
    			}    			
    			break;
    		} else{
    			// We have both ws and ag
    	    	WorkspaceDescriptor ws = wss.get(wssIndex);
    	    	AccessGroupStatus ag = ags.get(agsIndex);
    	    	int wsId = ws.wsId;
    	    	int agId = ag.getAccessGroupId().intValue();
    			
        		if(wsId == agId){
        			// We processed this workspace before
        			
        			if(ws.timestamp > ag.getTimestamp().longValue()){
        				// there are changes in the workspace
        				ws.ag = ag;
        				filteredWss.add(ws);        				
        				
        			} else{
        				// do not do anything, the ws is up to date.
        			}
        			// increment both if possible
        			wssIndex++;
        			agsIndex++;    			
        		} else if(wsId < agId){
        			// This worksapce was not processed before
    				ws.ag = null;
    				filteredWss.add(ws);        				        			
    				
    				// We will increment only ws index
    				wssIndex++;    			
        		} else if(wsId > agId){
        			//the workspace that was previously processed 
        			// is not in the list of workspaces to be processed now
        			
        			agsIndex++;
        		}    			
    		}    		 	
    	}
    	return filteredWss;
	}


	private void doWorkspacePresentObjects(WorkspaceDescriptor ws) throws IOException, JsonClientException {
    	WorkspaceClient wsClient = wsClient();
		    	
		ListObjectsParams params;
		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
		params = new ListObjectsParams().withIds( Arrays.asList((long)ws.wsId) );
    	params.setShowHidden(1L);
    	params.setShowAllVersions(1L);
    	
    	// We need to analyze data objects that were created after the last analyzed time 
    	if(ws.ag != null){
    		String dateFrom = Util.DATE_FORMATTER.print(ws.ag.getTimestamp());
    		params.withAfter(dateFrom);    		
    	}
    	
    	// The processing can take time, so we should analyze only those objects 
    	// that were created before the ws timestamp obtained at the time when the process was started  
		String dateTo = Util.DATE_FORMATTER.print( nextMoment(ws.timestamp) );
		params.withBefore(dateTo);
    	
    	rows = wsClient.listObjects(params);
    	
    	objectStatusEventsBuffer.clear();
    	buildEvents(ws, rows, ObjectStatusEventType.CREATED, objectStatusEventsBuffer, true);		
    	objectStatusChanged(objectStatusEventsBuffer);
	}
	
	private long nextMoment(long timestamp) {
		return timestamp + 1000;
	}

	private void doWorkspaceDeletedObjects(WorkspaceDescriptor ws) throws IOException, JsonClientException {
		
		System.out.println("doWorkspaceDeletedObjects ws:" + ws.wsId);
		
		// Process deleted objects in the present workspaces
    	WorkspaceClient wsClient = wsClient();
    	
		ListObjectsParams params;
		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
		params = new ListObjectsParams().withIds( Arrays.asList((long)ws.wsId) );
    	params.setShowAllVersions(1L);
    	params.setShowDeleted(1L);
    	params.setShowOnlyDeleted(1L);
    	
    	// We will process ALL deleted objects. Roman, sorry... you will have duplicated DELETED events
    	// Can be optimized in the future, but if we run reconstructor just once, then it is not necessary
    	rows = wsClient.listObjects(params);
    	
    	objectStatusEventsBuffer.clear();
    	buildEvents(ws, rows, ObjectStatusEventType.DELETED, objectStatusEventsBuffer, true);		
    	objectStatusChanged(objectStatusEventsBuffer);		
	}
	
	private void doDeletedWorkspace(WorkspaceDescriptor ws){
		// Can be implemented in the future, but it is not needed if we will run reconstructor just once,
		// because in this case we did NOT have objects CREATE events. 		
	}
	
	
	private void doWorkspaceDataPalettes(WorkspaceDescriptor ws) throws IOException, JsonClientException {
    	WorkspaceClient wsClient = wsClient();
    	
		ListObjectsParams params;
		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
		params = new ListObjectsParams().withIds( Arrays.asList((long)ws.wsId) );
    	params.setShowHidden(1L);
    	params.setShowAllVersions(1L);      
    	params.withType(DATA_PALETTE_TYPE);
    	
    	// We need to analyze all (!) Data Palettes in the workspace, thus we do not set "withAfter"    	
		String dateTo = Util.DATE_FORMATTER.print( nextMoment( ws.timestamp ) );
		params.withBefore(dateTo);
    	rows = wsClient.listObjects(params);
    	
    	objectDescriptorsBuffer.clear();
    	objectStatusEventsBuffer.clear();
    	buildObjectDescriptors(ws, rows, objectDescriptorsBuffer);
    	if(objectDescriptorsBuffer.size() > 0){
    		
        	reconstructDataPalletObjectStatusEvents(ws, objectDescriptorsBuffer, objectStatusEventsBuffer);
        	objectStatusChanged(objectStatusEventsBuffer);    	
    	}
	}
		
	private void processWorkspaceStates(List<WorkspaceDescriptor> wss, 
			List<AccessGroupStatus> accessGroupStates, boolean excludeNewlyProcessedWorskapces) throws IOException, JsonClientException {
		
		WorkspaceClient wsClient = wsClient();
		for(WorkspaceDescriptor ws : wss){
			if(excludeNewlyProcessedWorskapces && ws.ag == null) continue;
			if(ws.isDeleted) continue;
			
			if(ws.ag != null){
				if(ws.ag.isPrivate().booleanValue() && !ws.isPrivate){
					// WS became public: generate "published" events for all objects in WS
					createObjectsPublishedEvents(ws, true);
				} else if(!ws.ag.isPrivate().booleanValue() && ws.isPrivate){
					// WS became private: generate "unpublished" events for all objects in WS
					createObjectsPublishedEvents(ws, false);
				}				
			}
			// Get full list of users who can access this workspace
			WorkspaceIdentity params = new WorkspaceIdentity().withId((long)ws.wsId);
	    	@SuppressWarnings("deprecation")
            Map<String, String> usersMap = wsClient.getPermissions(params);
			
			AccessGroupStatus ag = new AccessGroupStatus(
					ws.ag != null ? ws.ag.getId() : null, 
					WS_STORAGE_CODE, 
					ws.wsId, 
					ws.timestamp, 
					ws.isPrivate,
					ws.isDeleted, 
					new LinkedList<>(usersMap.keySet()));
			accessGroupStates.add(ag);			
		}
	}	

	private void createObjectsPublishedEvents(WorkspaceDescriptor ws, boolean isPublishded) throws IOException, JsonClientException {
    	WorkspaceClient wsClient = wsClient();
    	
		ListObjectsParams params;
		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
		params = new ListObjectsParams().withIds( Arrays.asList((long)ws.wsId) );
    	params.setShowHidden(1L);
    	params.setShowAllVersions(1L);      
    	params.withType(DATA_PALETTE_TYPE);
    	
    	// We need to analyze all (!) Data Palettes in the workspace, thus we do not set "withAfter"    	
		String dateTo = Util.DATE_FORMATTER.print( nextMoment(  ws.timestamp ) );
		params.withBefore(dateTo);
    	rows = wsClient.listObjects(params);
    	
    	ObjectStatusEventType eventType = isPublishded ? ObjectStatusEventType.PUBLISHED : ObjectStatusEventType.UNPUBLISHED;
    	
    	objectStatusEventsBuffer.clear();
    	buildEvents(ws, rows, eventType, objectStatusEventsBuffer, true);		
    	objectStatusChanged(objectStatusEventsBuffer);
	}
	

	private void buildObjectDescriptors(WorkspaceDescriptor ws,
			List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows,
			List<ObjectDescriptor> objectDescriptors) {
		
    	for(Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>> row: rows){    		

    		String storageObjectType = row.getE3().split("-")[0];
    		ObjectDescriptor od = new ObjectDescriptor(
    				row.getE7().intValue(), 
    				row.getE1().toString(),  
    				row.getE5().intValue(),
    				storageObjectType,
    				Util.DATE_PARSER.parseDateTime(row.getE4()).getMillis(),
    				false);
    		
    		objectDescriptors.add(od);
    	}		
	}

	private void buildEvents(WorkspaceDescriptor ws, List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows,
			ObjectStatusEventType eventType, List<ObjectStatusEvent> objectStatusEvents, boolean excludeDataPallete) throws IOException {
		
    	for(Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>> row: rows){    		

    		StorageObjectType storageObjectType = new StorageObjectType(WS_STORAGE_CODE, row.getE3().split("-")[0]);
    		if( excludeDataPallete && storageObjectType.equals(DATA_PALETTE_TYPE) ) continue;
    		
    		ObjectStatusEvent event = new ObjectStatusEvent(
    				null,
    				WS_STORAGE_CODE,
    				row.getE7().intValue(), 
    				row.getE1().toString(),  
    				row.getE5().intValue(), 
    				null,
    				null,
    				Util.DATE_PARSER.parseDateTime(row.getE4()).getMillis(),
    				storageObjectType,    		
    				eventType,
    				!ws.isPrivate);
    		
    		objectStatusEvents.add(event);
    	}		
	}

    private void reconstructDataPalletObjectStatusEvents(
    		WorkspaceDescriptor ws,
    		List<ObjectDescriptor> dataPalettes, 
    		List<ObjectStatusEvent> events) throws IOException, JsonClientException {
    	
    	// Get data palette objects
    	WorkspaceClient wsClient = wsClient();    	
    	List<ObjectSpecification> objSpecs = new ArrayList<ObjectSpecification>();
    	for(ObjectDescriptor od: dataPalettes){
    		objSpecs.add(new ObjectSpecification().withRef(od.toRef()));
    	}
    	GetObjects2Params params = new GetObjects2Params();
        params.withObjects(objSpecs);
        GetObjects2Results ret = wsClient.getObjects2(params);
        
        List<DataPalette> dps = new ArrayList<DataPalette>();
        for(ObjectData data: ret.getData()){
        	dps.add(new DataPalette(data));
        }
        Collections.sort(dps);
        
        // Reconstruct history of events
        Set<String> curRefs = new HashSet<String>();         
        for(DataPalette dp: dps){
        	for(String ref: dp.refs){
        		if(!curRefs.contains(ref)){
        			// New reference
        			
        			// Do not create an event if it was analyzed already
        			if(ws.ag != null &&  dp.timestamp <= ws.ag.getTimestamp().longValue()) continue;
        				
        			String[] vals = ref.split("/");
        			ObjectStatusEvent event = new ObjectStatusEvent(
        					null, 
        					WS_STORAGE_CODE, 
        					Integer.parseInt(vals[0]),
        					vals[1],
        					Integer.parseInt(vals[2]),
        					null,
        					(int)dp.wsId,
        					dp.timestamp,        					
        					new StorageObjectType(WS_STORAGE_CODE, DATA_PALETTE_TYPE),
        					ObjectStatusEventType.SHARED,
        					!ws.isPrivate);
        				events.add(event);
        		} else{
        			// Reference existed already
        			curRefs.remove(ref);
        		}
        	}
        	// All that were not found are unshared
        	for(String ref: curRefs){
        		
    			// Do not create an event if it was analyzed already
    			if(ws.ag != null &&  dp.timestamp < ws.ag.getTimestamp().longValue()) continue;
        		
    			String[] vals = ref.split("/");
    			ObjectStatusEvent event = new ObjectStatusEvent(
    					null, 
    					WS_STORAGE_CODE, 
    					Integer.parseInt(vals[0]),
    					vals[1],
    					Integer.parseInt(vals[2]),
    					null,
    					(int)dp.wsId,
    					dp.timestamp,        					
    					new StorageObjectType(WS_STORAGE_CODE, DATA_PALETTE_TYPE),
    					ObjectStatusEventType.UNSHARED,
    					!ws.isPrivate);
    			events.add(event);        		
        	}
        	// Set up the current state
        	curRefs.clear();
        	curRefs.addAll(Arrays.asList(dp.refs));
        }
	}

    
	private WorkspaceDescriptor getWorkspacesDescriptor(Long wsId) throws IOException, JsonClientException{
		
    	WorkspaceClient wsClient = wsClient();    	
    	WorkspaceIdentity params = new WorkspaceIdentity();
    	params.setId(wsId);
		Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo = wsClient.getWorkspaceInfo(params);
    	
		// We do not know whether it is deleted/private
		return new  WorkspaceDescriptor(
				wsInfo.getE1().intValue(), 
				Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis(),
				false, false);		
    }
    
	private List<WorkspaceDescriptor> getWorkspacesDescriptors(AccessType wsAccessStatus, PresenceType objPresenceType, Set<Long> excludedWsIds) throws IOException, JsonClientException{
		
		List<WorkspaceDescriptor> wss = new ArrayList<WorkspaceDescriptor>();
    	ListWorkspaceInfoParams params = new ListWorkspaceInfoParams();

    	WorkspaceClient wsClient = wsClient();
    	
    	// We can not get public workspaces only, so we need workaround
    	Set<Long> privateWsIds = new HashSet<Long>();
		
    	List<Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>>> wsInfos;

    	boolean accessStatusValid = false;
    	boolean presenceStatusValid = false;
    	
    	// Private && present 		
    	params.setExcludeGlobal(1L);
    	params.setShowDeleted(0L);
    	params.setShowOnlyDeleted(0L);

		accessStatusValid = wsAccessStatus == AccessType.PRIVATE || wsAccessStatus == AccessType.ALL;
//		presenceStatusValid = objPresenceType == PresenceType.PRESENT || objPresenceType == PresenceType.ALL;

		// Both present and deleted objects can be in present workspace
		presenceStatusValid = true;
		
    	wsInfos = wsClient.listWorkspaceInfo(params);    	
    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){
    		privateWsIds.add(wsInfo.getE1());    		
    		if( accessStatusValid && presenceStatusValid){
    			if(excludedWsIds != null && excludedWsIds.contains(wsInfo.getE1())) continue;
        		wss.add( new WorkspaceDescriptor(
        				wsInfo.getE1().intValue(), 
        				Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis(),
        				true, false));    		    			    			
    		}    		
    	}
    	    	
    	// Private && deleted    	
    	params.setExcludeGlobal(1L);
    	params.setShowDeleted(1L);
    	params.setShowOnlyDeleted(1L);
    	
		accessStatusValid = wsAccessStatus == AccessType.PRIVATE || wsAccessStatus == AccessType.ALL;
		presenceStatusValid = objPresenceType == PresenceType.DELETED || objPresenceType == PresenceType.ALL; 
    	
    	wsInfos = wsClient.listWorkspaceInfo(params);    	
    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){
    		privateWsIds.add(wsInfo.getE1());
    		
    		
    		if( accessStatusValid && presenceStatusValid){
    			if(excludedWsIds != null && excludedWsIds.contains(wsInfo.getE1())) continue;
        		wss.add( new WorkspaceDescriptor(
        				wsInfo.getE1().intValue(), 
        				Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis(),
        				true, true));    		    			    			
    		}    		
    	}
    	
    	// Public && present
    	
		accessStatusValid = wsAccessStatus == AccessType.PUBLIC || wsAccessStatus == AccessType.ALL;
//		presenceStatusValid = objPresenceType == PresenceType.PRESENT || objPresenceType == PresenceType.ALL; 

		// Both present and deleted objects can be in present workspace
		presenceStatusValid = true;		
    	
		if(accessStatusValid && presenceStatusValid){
			
	    	params.setExcludeGlobal(0L);
	    	params.setShowDeleted(0L);
	    	params.setShowOnlyDeleted(0L);

	    	wsInfos = wsClient.listWorkspaceInfo(params);    	
	    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){
	    		if(excludedWsIds != null &&  excludedWsIds.contains(wsInfo.getE1()) ) continue;
	    		if( privateWsIds.contains(wsInfo.getE1()) ) continue;
        		wss.add( new WorkspaceDescriptor(
        				wsInfo.getE1().intValue(), 
        				Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis(),
        				false, false));    		    			    				    		    		
	    	}
		}

    	// Public &&  deleted
		accessStatusValid = wsAccessStatus == AccessType.PUBLIC || wsAccessStatus == AccessType.ALL;
		presenceStatusValid = objPresenceType == PresenceType.DELETED || objPresenceType == PresenceType.ALL; 
		
		if(accessStatusValid && presenceStatusValid){
			
	    	params.setExcludeGlobal(0L);
	    	params.setShowDeleted(1L);
	    	params.setShowOnlyDeleted(1L);

	    	wsInfos = wsClient.listWorkspaceInfo(params);    	
	    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){
	    		if(excludedWsIds != null &&  excludedWsIds.contains(wsInfo.getE1()) ) continue;
	    		if( privateWsIds.contains(wsInfo.getE1()) ) continue;
        		wss.add( new WorkspaceDescriptor(
        				wsInfo.getE1().intValue(), 
        				Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis(),
        				false, true));    		    			    				    		    		
	    	}
		}
		
        return wss;
    }

	private void objectStatusChanged(List<ObjectStatusEvent> events) throws IOException{
		for(StatusEventListener listener: eventListeners){
			listener.objectStatusChanged(events);
		}
	}

	private void groupStatusChanged(List<AccessGroupStatus> newStatuses) throws IOException{
		for(StatusEventListener listener: eventListeners){
			listener.groupStatusChanged(newStatuses);
		}
	}
	
	private void groupPermissionsChanged(List<AccessGroupStatus> newStatuses) throws IOException{
		for(StatusEventListener listener: eventListeners){
			listener.groupPermissionsChanged(newStatuses);
		}
	}
	
    public WorkspaceClient wsClient() throws IOException{
    	if(_wsClient == null){
    		
    		try {
				_wsClient = new WorkspaceClient(wsUrl, token);
			} catch (UnauthorizedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
            _wsClient.setIsInsecureHttpConnectionAllowed(true); 		
    	}
    	return _wsClient;
    }
}
