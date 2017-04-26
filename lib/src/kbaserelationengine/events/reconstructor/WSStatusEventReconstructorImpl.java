package kbaserelationengine.events.reconstructor;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.ObjectStatusEvent;
import kbaserelationengine.events.ObjectStatusEventType;
import kbaserelationengine.events.WSStatusEventTrigger;
import kbaserelationengine.events.storage.StatusEventStorage;
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

public class WSStatusEventReconstructorImpl implements WSStatusEventReconstructor{
	
	private static String WS_STORAGE_CODE = "WS";
	private static String DATA_PALETTE_TYPE = "DataPalette.DataPalette";
//	private static final Object NARRATIVE_TYPE = "KBaseNarrative.Narrative";
	
	private URL wsUrl;
	private AuthToken token;
	private StatusEventStorage objStatusStorage;
	private WSStatusEventTrigger eventTrigger;
	
	private WorkspaceClient _wsClient;
	private List<ObjectStatusEvent> objectStatusEventsBuffer = new ArrayList<ObjectStatusEvent>();
	private List<ObjectDescriptor> objectDescriptorsBuffer = new ArrayList<ObjectDescriptor>();
	
	
	public WSStatusEventReconstructorImpl(URL wsUrl, AuthToken token, 
			StatusEventStorage objStatusStorage,
			WSStatusEventTrigger eventTrigger) {
		this.wsUrl = wsUrl;
		this.token = token;
		this.objStatusStorage = objStatusStorage;
		this.eventTrigger = eventTrigger;
	}
		
	@Override
	public void processWorkspaceObjects(
			AccessType wsAccessType, 
			PresenceType objPresenceType,
			Set<Long> excludedWsIds) throws IOException{
		try {
			// Get history: accessGroups (workspaces) processed before
			List<AccessGroupStatus> ags = objStatusStorage.findAccessGroups(WS_STORAGE_CODE);
			Collections.sort(ags);
			
			// Get current state: workspaces status  
	    	List<WorkspaceDescriptor> wss = getWorkspacesDescriptors(wsAccessType, objPresenceType, excludedWsIds);
	    	Collections.sort(wss);
	    	
	    	// Filter by timestamps
	    	List<WorkspaceDescriptor> filteredWss = filterByTimestamps(wss, ags);

	    	
	    	// Let us first implement non-deleted
	    	if(objPresenceType != PresenceType.PRESENT) return;
	    	
	    	// Phase 1: do all objects 
	    	for(WorkspaceDescriptor ws: filteredWss){
	    		doWorkspaceObjects(ws); 
	    	}
	    	
	    	// Phase 2: do all data palettes 
	    	for(WorkspaceDescriptor ws: filteredWss){
	    		doWorkspaceDataPalettes(ws);
	    	}
	    	
    		// Phase 3: update all workspace states
	    	for(WorkspaceDescriptor ws: filteredWss){
	    		updateWorskapceState(ws);
	    	}
	    	
		} catch (JsonClientException e) {
			throw new IOException(e);
		}		
		
	};
	


	@Override
	public void processWorkspacePermissions(
			AccessType wsAccessType, 
			Set<Long> excludedWsIds) throws IOException{
		
		try {
	    	List<WorkspaceDescriptor> wss = getWorkspacesDescriptors(wsAccessType, PresenceType.ALL, excludedWsIds);
	    	for(WorkspaceDescriptor ws: wss){
	    		updateWorskapceState(ws);
	    	}
			
		} catch (JsonClientException e) {
			throw new IOException(e);
		}		
	};
	
		
	private List<WorkspaceDescriptor> filterByTimestamps(List<WorkspaceDescriptor> wss, List<AccessGroupStatus> ags) {
		
		List<WorkspaceDescriptor> filteredWss = new ArrayList<WorkspaceDescriptor>();
		
		// Both lists are expected to be sorted, but will sort them just ofr the case
		Collections.sort(wss);
		Collections.sort(ags);
				
    	int wssIndex = 0;
    	int agsIndex = 0;
    	
    	WorkspaceDescriptor ws = wssIndex < wss.size() ? wss.get(wssIndex) : null;
    	AccessGroupStatus ag = agsIndex < ags.size() ? ags.get(agsIndex) : null;
    	
    	int wsId = ws != null ? ws.wsId : -1;
    	int agId = ag != null ? ag.getAccessGroupId().intValue() : -1;
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
        			//not possible... may be throw exception? 
        			throw new RuntimeException("wsId > agId");
//        			agsIndex++;
        		}    			
    		}    		 	
    	}
    	return filteredWss;
	}

	private void updateWorskapceState(WorkspaceDescriptor ws) throws IOException, JsonClientException {
		WorkspaceClient wsClient = wsClient();
		
		// Get full list of users who can access this workspace
		WorkspaceIdentity params = new WorkspaceIdentity().withId((long)ws.wsId);
    	Map<String, String> usersMap = wsClient.getPermissions(params);
		String[] users = usersMap.keySet().toArray(new String[usersMap.size()]);
		
		AccessGroupStatus ag = new AccessGroupStatus(
				ws.ag != null ? ws.ag.getId() : null, 
				WS_STORAGE_CODE, 
				ws.wsId, 
				ws.timestamp, 
				ws.isPrivate,
				ws.isDeleted, 
				users);
		eventTrigger.trigger(ag);			
	}

	private void doWorkspaceObjects(WorkspaceDescriptor ws) throws IOException, JsonClientException {
    	WorkspaceClient wsClient = wsClient();
		    	
		ListObjectsParams params;
		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
		params = new ListObjectsParams().withIds( Arrays.asList((long)ws.wsId) );
    	params.setShowHidden(1L);
    	params.setShowAllVersions(1L);
    	
    	// We need to analyze data objects that were created after the last analyzed time and before 
    	// the time the workspace was updated last time before the start of processing procedure!
    	if(ws.ag != null){
    		String dateFrom = Util.DATE_FORMATTER.print(ws.ag.getTimestamp());
    		params.withAfter(dateFrom);    		
    	}
		String dateTo = Util.DATE_FORMATTER.print(ws.timestamp);
		params.withBefore(dateTo);
    	
    	rows = wsClient.listObjects(params);
    	
    	objectStatusEventsBuffer.clear();
    	buildEvents(ws, rows, ObjectStatusEventType.CREATED, objectStatusEventsBuffer, true);		
    	triggerEvents(objectStatusEventsBuffer);
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
		String dateTo = Util.DATE_FORMATTER.print(ws.timestamp);
		params.withBefore(dateTo);
    	rows = wsClient.listObjects(params);
    	
    	objectDescriptorsBuffer.clear();
    	objectStatusEventsBuffer.clear();
    	buildObjectDescriptors(ws, rows, objectDescriptorsBuffer);
    	if(objectDescriptorsBuffer.size() > 0){
        	reconstructDataPalletObjectStatusEvents(ws, objectDescriptorsBuffer, objectStatusEventsBuffer);
        	triggerEvents(objectStatusEventsBuffer);    	
    	}
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

//	private void doWorkspace(WorkspaceDescriptor ws, 
//			AccessGroupStatus ag,
//			PresenceType objPresenceType,
//			List<ObjectStatusEvent> dataPalettes) throws IOException, JsonClientException {
//		
//    	WorkspaceClient wsClient = wsClient();
//		
//    	List<ObjectStatusEvent> objectStatusEvents = new ArrayList<ObjectStatusEvent>();
//    	
//		ListObjectsParams params;
//		List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows;
//		
//		
//		
//		// Do present objects: events will be generated only for NEW objects, after last TIMESTAMP
//		if(objPresenceStatus == PresenceType.PRESENT || objPresenceStatus == PresenceType.ALL){
//			params = new ListObjectsParams().withIds( Arrays.asList((long)ws.wsId) );
//	    	params.setShowHidden(1L);
//	    	params.setShowAllVersions(1L);        	
//	    	if(lastTimestamp != null){
//	    		String lastDate = Util.DATE_FORMATTER.print(lastTimestamp);
//	    		params.withAfter(lastDate);
//	    	}
//	    	rows = wsClient.listObjects(params);
//			createEvents(rows, ObjectStatusEventType.CREATED, objectStatusEvents, objType);			
//		}
//		
//		// Do deleted objects: we can not use TIMESTAMP, thus events for ALL deleted objects will
//		// be generated
//		if(objPresenceStatus == PresenceType.DELETED || objPresenceStatus == PresenceType.ALL){
//	    	params = new ListObjectsParams().withIds( Arrays.asList((long)ws.wsId) );
//	    	params.setShowAllVersions(1L);
//	    	params.setShowDeleted(1L);
//	    	params.setShowOnlyDeleted(1L);
//	    	rows = wsClient.listObjects(params);
//			createEvents(rows, ObjectStatusEventType.DELETED, objectStatusEvents, objType);
//		}
//		
//		triggerEvents(objectStatusEvents);
//		updateWorskapceState(ws, wsStorageRowId);
//	}

	private void triggerEvents(List<ObjectStatusEvent> objectStatusEvents) throws IOException {
		for(ObjectStatusEvent event: objectStatusEvents){
			eventTrigger.trigger(event);
		}		
	}

//	private void updateWorskapceState(WorkspaceDescriptor ws, String wsdbRowId) throws IOException, JsonClientException {
//    	WorkspaceClient wsClient = wsClient();
//		
//		// Get full list of users who can access this workspace
//		WorkspaceIdentity params = new WorkspaceIdentity().withId((long)ws.wsId);
//    	Map<String, String> usersMap = wsClient.getPermissions(params);
//		String[] users = usersMap.keySet().toArray(new String[usersMap.size()]);
//		
//		AccessGroupStatus ag = new AccessGroupStatus(
//				wsdbRowId, 
//				WS_STORAGE_CODE, 
//				ws.wsId, 
//				ws.timestamp, 
//				ws.isDeleted, 
//				users);
//		eventTrigger.trigger(ag);		
//	}

	private void buildEvents(WorkspaceDescriptor ws, List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> rows,
			ObjectStatusEventType eventType, List<ObjectStatusEvent> objectStatusEvents, boolean ecludeDataPallete) throws IOException {
		
    	for(Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>> row: rows){    		

    		String storageObjectType = row.getE3().split("-")[0];
    		if( ecludeDataPallete && storageObjectType.equals(DATA_PALETTE_TYPE) ) continue;
    		
    		ObjectStatusEvent event = new ObjectStatusEvent(
    				null,
    				WS_STORAGE_CODE,
    				row.getE7().intValue(), 
    				row.getE1().toString(),  
    				row.getE5().intValue(), 
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
        			if(ws.ag != null &&  dp.timestamp < ws.ag.getTimestamp().longValue()) continue;
        				
        			String[] vals = ref.split("/");
        			ObjectStatusEvent event = new ObjectStatusEvent(
        					null, 
        					WS_STORAGE_CODE, 
        					Integer.parseInt(vals[0]),
        					vals[1],
        					Integer.parseInt(vals[2]),
        					(int)dp.wsId,
        					dp.timestamp,        					
        					DATA_PALETTE_TYPE,
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
    					(int)dp.wsId,
    					dp.timestamp,        					
    					DATA_PALETTE_TYPE,
    					ObjectStatusEventType.UNSHARED,
    					!ws.isPrivate);
    			events.add(event);        		
        	}
        	// Set up the current state
        	curRefs.clear();
        	curRefs.addAll(Arrays.asList(dp.refs));
        	        	
//        	System.out.println(dp.wsId + "/" + dp.objId + "/" + dp.version + ": " + dp.timestamp);
        }
        
//        System.out.println("=== Events");
//        for(ObjectStatusEvent event: events){
//        	System.out.println(event);
//        }               
	}

	private List<WorkspaceDescriptor> getWorkspacesDescriptors(AccessType wsAccessStatus, PresenceType objPresenceType, Set<Long> excludedWsIds) throws IOException, JsonClientException{
		
		List<WorkspaceDescriptor> wss = new ArrayList<WorkspaceDescriptor>();
    	ListWorkspaceInfoParams params = new ListWorkspaceInfoParams();

    	WorkspaceClient wsClient = wsClient();
    	
    	// We can not get public workspaces only, so we need workaround
    	Set<Long> privateWsIds = new HashSet<Long>();
		
    	List<Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>>> wsInfos;

    	
    	// Private && non deleted 
    	params.setExcludeGlobal(1L);
    	params.setShowDeleted(0L);
    	params.setShowOnlyDeleted(0L);

    	wsInfos = wsClient.listWorkspaceInfo(params);    	
    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){
    		privateWsIds.add(wsInfo.getE1());
    		
    		if( (wsAccessStatus == AccessType.PRIVATE || wsAccessStatus == AccessType.ALL) &&
    			(objPresenceType == PresenceType.PRESENT || objPresenceType == PresenceType.ALL)){    			
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
    	wsInfos = wsClient.listWorkspaceInfo(params);    	
    	for(Tuple9<Long, String, String, String, Long, String, String, String, Map<String, String>> wsInfo: wsInfos){
    		privateWsIds.add(wsInfo.getE1());
    		
    		if( (wsAccessStatus == AccessType.PRIVATE || wsAccessStatus == AccessType.ALL) &&
    			(objPresenceType == PresenceType.DELETED || objPresenceType == PresenceType.ALL)){    			
    			if(excludedWsIds != null && excludedWsIds.contains(wsInfo.getE1())) continue;
        		wss.add( new WorkspaceDescriptor(
        				wsInfo.getE1().intValue(), 
        				Util.DATE_PARSER.parseDateTime(wsInfo.getE4()).getMillis(),
        				true, true));    		    			    			
    		}    		
    	}
    	
    	// Public && non deleted
		if( (wsAccessStatus == AccessType.PUBLIC || wsAccessStatus == AccessType.ALL) &&
    			(objPresenceType == PresenceType.PRESENT || objPresenceType == PresenceType.ALL)){
			
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
		if( (wsAccessStatus == AccessType.PUBLIC || wsAccessStatus == AccessType.ALL) &&
    			(objPresenceType == PresenceType.DELETED || objPresenceType == PresenceType.ALL)){
			
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
