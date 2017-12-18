package kbasesearchengine.test.events.handler;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

import kbasesearchengine.events.handler.CloneableWorkspaceClient;
import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import workspace.WorkspaceClient;

public class CloneableWorkspaceClientImplTest {
    
    private static AuthToken userToken;
    
    @BeforeClass
    public static void getToken() throws Exception {
        final URL authURL = TestCommon.getAuthUrl();
        final ConfigurableAuthService authSrv = new ConfigurableAuthService(
                new AuthConfig().withKBaseAuthServerURL(authURL));
        userToken = TestCommon.getToken(authSrv);
    }
    
    @Test
    public void clientIdentity() throws Exception {
        final WorkspaceClient wc = new WorkspaceClient(new URL("https://foo.com"), userToken);
        final CloneableWorkspaceClient cl = new CloneableWorkspaceClientImpl(wc);
        assertThat("non-identical instance", cl.getClient(), sameInstance(wc));
    }
    
    @Test
    public void cloneDefault() throws Exception {
        final WorkspaceClient wc = new WorkspaceClient(new URL("https://foo.com"), userToken);
        final CloneableWorkspaceClient cl = new CloneableWorkspaceClientImpl(wc);
        final WorkspaceClient clone = cl.getClientClone();
        
        assertThat("identical instance", clone, not(sameInstance(wc)));
        assertThat("incorrect url", clone.getURL(), is(new URL("https://foo.com")));
        assertThat("incorrect token", clone.getToken(), is(userToken));
        assertThat("incorrect insecure setting", clone.isInsecureHttpConnectionAllowed(),
                is(false));
    }
    
    @Test
    public void cloneSecureFalse() throws Exception {
        final WorkspaceClient wc = new WorkspaceClient(new URL("https://foo2.com"), userToken);
        wc.setIsInsecureHttpConnectionAllowed(true);
        final CloneableWorkspaceClient cl = new CloneableWorkspaceClientImpl(wc);
        final WorkspaceClient clone = cl.getClientClone();
        
        assertThat("identical instance", clone, not(sameInstance(wc)));
        assertThat("incorrect url", clone.getURL(), is(new URL("https://foo2.com")));
        assertThat("incorrect token", clone.getToken(), is(userToken));
        assertThat("incorrect insecure setting", clone.isInsecureHttpConnectionAllowed(),
                is(true));
    }

}
