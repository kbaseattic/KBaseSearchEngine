package kbasesearchengine.main;

import java.io.IOException;
import java.util.Map;

import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.UObject;
import us.kbase.workspace.ListWorkspaceIDsParams;
import us.kbase.workspace.ListWorkspaceIDsResults;
import us.kbase.workspace.WorkspaceClient;

/** A provider for KBase workspace and narrative info.
 * @author gaprice@lbl.gov
 *
 */
public class WorkspaceNarrativeInfoProvider implements NarrativeInfoProvider {

    //TODO TEST

    /* wsHandler created with an event handler pointing at the workspace from which data should
     * be retrieved. This should be the same workspace as that from which the data is indexed.
     */

    private final WorkspaceEventHandler wsHandler;

    /** Create the provider.
     * @param wsClient a workspace client initialized with administrator credentials.
     */
    public WorkspaceNarrativeInfoProvider(final WorkspaceClient wsClient) {
        if (wsClient == null) {
            throw new NullPointerException("WorkspaceClient");
        }
        this.wsHandler = new WorkspaceEventHandler(new CloneableWorkspaceClientImpl(wsClient));
    }

    /** For the given workspace ID, returns workspace info related to the narrative.
     * @param wsid workspace id.
     */
    @Override
    public NarrativeInfo findNarrativeInfo(final Long wsid)
            throws IOException, JsonClientException {
        final Tuple9 <Long, String, String, String, Long, String, String,
                String, Map<String,String>> wsInfo;

        try {
            wsInfo = wsHandler.getWorkspaceInfo(wsid);
        } catch (IOException e) {
            throw new IOException("Failed retrieving workspace info: " + e.getMessage(), e);
        } catch (JsonClientException e) {
            throw new JsonClientException("Failed retrieving workspace info: "
                    + e.getMessage(), e);
        }

        final long timeMilli = WorkspaceEventHandler.parseDateToEpochMillis(wsInfo.getE4());

        final NarrativeInfo tempNarrInfo =
                new NarrativeInfo()
                        .withTimeLastSaved(timeMilli)         // modification time
                        .withWsOwnerUsername(wsInfo.getE3()); // workspace user name

        final Map<String, String> wsInfoMeta = wsInfo.getE9();

        if (wsInfoMeta.containsKey("narrative") &&
                wsInfoMeta.containsKey("narrative_nice_name")) {
            tempNarrInfo.withNarrativeName(wsInfoMeta.get("narrative_nice_name"))
                    .withNarrativeId(Long.parseLong(wsInfoMeta.get("narrative")));
        }
        return tempNarrInfo;
    }
}
