package kbasesearchengine.events.handler;

import us.kbase.workspace.WorkspaceClient;

/** A wrapper class for a workspace client that implements a clone method. This produces
 * a new workspace client with the same url and token as the client returned by getClient() and
 * the same setting for {@link WorkspaceClient#isInsecureHttpConnectionAllowed()}.
 * @author gaprice@lbl.gov
 *
 */
public interface CloneableWorkspaceClient {
    
    /** Returns the workspace client from which clones are produced.
     * @return the original workspace client.
     */
    WorkspaceClient getClient();
    
    /** Returns a new workspace client with the same url, token, and insecure http setting
     * as the workspace client returned by {@link #getClient()}.
     * @return a cloned workspace client.
     */
    WorkspaceClient getClientClone();

}
