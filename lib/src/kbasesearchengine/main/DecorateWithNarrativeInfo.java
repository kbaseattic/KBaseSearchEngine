package kbasesearchengine.main;

import kbasesearchengine.ObjectData;
import kbasesearchengine.common.GUID;
import kbasesearchengine.tools.Utils;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.exceptions.RetriableIndexingException;
import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.SearchTypesInput;
import kbasesearchengine.SearchTypesOutput;
import kbasesearchengine.SearchObjectsInput;
import kbasesearchengine.SearchObjectsOutput;
import kbasesearchengine.GetObjectsInput;
import kbasesearchengine.GetObjectsOutput;
import kbasesearchengine.TypeDescriptor;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.Tuple9;
import us.kbase.workspace.WorkspaceClient;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

/**
 * Created by umaganapathy on 1/26/18.
 */
public class DecorateWithNarrativeInfo implements SearchInterface {

    private final static DateTimeFormatter DATE_PARSER =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss"))
                    .appendOptional(DateTimeFormat.forPattern(".SSS").getParser())
                    .append(DateTimeFormat.forPattern("Z"))
                    .toFormatter();

    /** The storage code for workspace events. */
    public static final String STORAGE_CODE = "WS";
    private final WorkspaceEventHandler weh;
    private final SearchInterface searchInterface;

    public DecorateWithNarrativeInfo(
            final SearchInterface searchInterface,
            final WorkspaceClient wsClient) {
        Utils.nonNull(searchInterface, "searchInterface");
        Utils.nonNull(wsClient, "wsClient");
        this.searchInterface = searchInterface;
        this.weh = new WorkspaceEventHandler(
                new CloneableWorkspaceClientImpl(wsClient));
    }

    public SearchTypesOutput searchTypes(
            final SearchTypesInput params,
            final String user)
            throws Exception {
        return searchInterface.searchTypes(params, user);
    }

    public Map<String, TypeDescriptor> listTypes(
            final String uniqueType)
            throws Exception {
        return searchInterface.listTypes(uniqueType);
    }

    public SearchObjectsOutput searchObjects(
            final SearchObjectsInput params,
            final String user)
            throws Exception {
        SearchObjectsOutput searchObjsOutput = searchInterface.searchObjects(params, user);
        searchObjsOutput.setAccessGroupNarrativeInfo(addNarrativeInfo(searchObjsOutput.getObjects(),
                searchObjsOutput.getAccessGroupNarrativeInfo()));
        return searchObjsOutput;
    }

    public GetObjectsOutput getObjects(
            final GetObjectsInput params,
            final String user)
            throws Exception {
        GetObjectsOutput getObjsOutput = searchInterface.getObjects(params, user);
        getObjsOutput.setAccessGroupNarrativeInfo(addNarrativeInfo(getObjsOutput.getObjects(),
                getObjsOutput.getAccessGroupNarrativeInfo()));
        return getObjsOutput;
    }

    private Map<Long, Tuple5 <String, Long, Long, String, String>> addNarrativeInfo(
            final List<ObjectData> objects,
            final Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrInfo)
            throws RetriableIndexingException, IndexingException, ParseException {

        final Map<Long, Tuple5 <String, Long, Long, String, String>> retVal = new HashMap<>();

        if (accessGroupNarrInfo != null) {
            retVal.putAll(accessGroupNarrInfo);
        }

        Set<Long> wsIdsSet = new HashSet<>();

        for (final ObjectData objData: objects) {

            final GUID guid = new GUID(objData.getGuid());

            final String storageCode = guid.getStorageCode();

            if (STORAGE_CODE.equals(storageCode)) {
                final long wsId = guid.getAccessGroupId();
                wsIdsSet.add(wsId);
            }
        }
        for (final long workspaceId: wsIdsSet) {
            if (!retVal.containsKey(workspaceId)) {
                final Tuple9 <Long, String, String, String, Long, String, String,
                        String, Map<String,String>> wsInfo;
                final Tuple5 <String, Long, Long, String, String> tempNarrInfo =
                        new Tuple5<>();
                // get workspace info meta data
                wsInfo = weh.getWorkspaceInfo(workspaceId);

                final Instant timeMilli = Instant.ofEpochMilli(DATE_PARSER.
                        parseDateTime(wsInfo.getE4()).getMillis());

                tempNarrInfo.setE3(timeMilli.toEpochMilli());  // modification time
                tempNarrInfo.setE4(wsInfo.getE3());            // workspace user name
                tempNarrInfo.setE5("");                        // TODO workspace user, real name

                final Map<String, String> wsInfoMeta = wsInfo.getE9();

                if ( wsInfoMeta.containsKey("narrative") &&
                        wsInfoMeta.containsKey("narrative_nice_name") ) {
                    tempNarrInfo.setE1(wsInfoMeta.get("narrative_nice_name"));
                    tempNarrInfo.setE2(Long.parseLong(wsInfoMeta.get("narrative")));
                }
                else {
                    tempNarrInfo.setE1(null);       // narrative name not available
                    tempNarrInfo.setE2(null);       // narrative id not available
                }
                retVal.put(workspaceId, tempNarrInfo);
            }
        }
        return retVal;
    }
}
