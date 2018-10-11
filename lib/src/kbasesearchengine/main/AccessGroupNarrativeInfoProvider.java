package kbasesearchengine.main;

import java.io.IOException;
import java.util.Map;

import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple9;
import us.kbase.common.service.UObject;
import us.kbase.workspace.ListWorkspaceIDsParams;
import us.kbase.workspace.ListWorkspaceIDsResults;
import us.kbase.workspace.WorkspaceClient;
import org.slf4j.LoggerFactory;

/** A provider for KBase workspace and narrative info.
 * @author gaprice@lbl.gov
 *
 */
public class AccessGroupNarrativeInfoProvider implements NarrativeInfoProvider {

    //TODO TEST

    /* wsHandler created with an event handler pointing at the workspace from which data should
     * be retrieved. This should be the same workspace as that from which the data is indexed.
     */

    private final WorkspaceEventHandler wsHandler;

    /** Create the provider.
     * @param wsClient a workspace client initialized with administrator credentials.
     */
    public AccessGroupNarrativeInfoProvider(final WorkspaceEventHandler wsHandler) {
        Utils.nonNull(wsHandler, "WorkspaceHandler");
        this.wsHandler = wsHandler;
    }

    /** For the given access group ID, returns workspace info related to the narrative
     * or null if no workspace info could be retrieved or found.
     * @param wsid workspace id.
     */
    @Override
    public NarrativeInfo findNarrativeInfo(final Long accessGroupID) throws IOException, JsonClientException {
        final Tuple9 <Long, String, String, String, Long, String, String,
                String, Map<String,String>> wsInfo;

            try {
                wsInfo = wsHandler.getWorkspaceInfo(accessGroupID);
            } catch (IOException e) {
                if (e.getMessage().toLowerCase().contains("is deleted") ||
                        e.getMessage().toLowerCase().contains("has been deleted")) {
                    return null;
                } else {
                    throw e;
                }
            } catch (JsonClientException e) {
               throw e;
            } catch (Exception e) {
                //should not occur
                LoggerFactory.getLogger(getClass()).error("ERROR: Failed retrieving workspace info: {}",
                        e.getMessage());
                return null;
            }

        final long timeMilli = WorkspaceEventHandler.parseDateToEpochMillis(wsInfo.getE4());

        final NarrativeInfo tempNarrInfo =
                new NarrativeInfo(null, null, timeMilli, wsInfo.getE3());

        final Map<String, String> wsInfoMeta = wsInfo.getE9();

        if (wsInfoMeta.containsKey("narrative") &&
                wsInfoMeta.containsKey("narrative_nice_name")) {
            tempNarrInfo.withNarrativeName(wsInfoMeta.get("narrative_nice_name"))
                    .withNarrativeId(Long.parseLong(wsInfoMeta.get("narrative")));
        }
        return tempNarrInfo;
    }
}
