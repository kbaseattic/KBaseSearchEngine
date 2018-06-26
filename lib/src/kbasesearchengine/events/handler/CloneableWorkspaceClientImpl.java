package kbasesearchengine.events.handler;

import java.io.IOException;

import kbasesearchengine.tools.Utils;
import us.kbase.common.service.UnauthorizedException;
import us.kbase.workspace.WorkspaceClient;

/** The standard implementation of {@link CloneableWorkspaceClient}.
 * @author gaprice@lbl.gov
 *
 */
public class CloneableWorkspaceClientImpl implements CloneableWorkspaceClient {
    
    private final WorkspaceClient ws;
    
    /** Create a new cloneable client.
     * @param ws the workspace client for which cloning is required.
     */
    public CloneableWorkspaceClientImpl(final WorkspaceClient ws) {
        Utils.nonNull(ws, "ws");
        this.ws = ws;
    }

    @Override
    public WorkspaceClient getClient() {
        return ws;
    }

    @Override
    public WorkspaceClient getClientClone() {
        try {
            final WorkspaceClient wc = new WorkspaceClient(ws.getURL(), ws.getToken());
            wc.setIsInsecureHttpConnectionAllowed(ws.isInsecureHttpConnectionAllowed());
            return wc;
        } catch (IOException | UnauthorizedException e) {
            throw new RuntimeException("As of 11d/12m/17y this exception cannot be thrown when " +
                    "creating a workspace client. Check the code");
        }
    }

}
