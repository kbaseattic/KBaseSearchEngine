package kbasesearchengine.events;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.UObject;
import workspace.ListWorkspaceIDsParams;
import workspace.ListWorkspaceIDsResults;
import workspace.WorkspaceClient;

/** A provider for KBase workspace service based access group IDs.
 * @author gaprice@lbl.gov
 *
 */
public class WorkspaceAccessGroupProvider implements AccessGroupProvider {
    
    //TODO TEST
    
    private final WorkspaceClient ws;
    
    /** Create the provider.
     * @param adminClient a workspace client initialized with administrator credentials.
     */
    public WorkspaceAccessGroupProvider(final WorkspaceClient adminClient) {
        if (adminClient == null) {
            throw new NullPointerException("adminClient");
        }
        this.ws = adminClient;
    }

    @Override
    public List<Integer> findAccessGroupIds(String user) throws IOException {
        try {
            return ws.administer(new UObject(ImmutableMap.of(
                    "command", "listWorkspaceIDs",
                    "params", new ListWorkspaceIDsParams(),
                    "user", user)))
                    .asClassInstance(ListWorkspaceIDsResults.class)
                    .getWorkspaces().stream().map(l -> Math.toIntExact(l))
                            .collect(Collectors.toList());
        } catch (JsonClientException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

}
