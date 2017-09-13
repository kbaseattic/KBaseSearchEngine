package kbasesearchengine.test;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;


import org.ini4j.Ini;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import kbasesearchengine.KBaseSearchEngineServer;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import us.kbase.common.service.JsonServerSyslog;
import us.kbase.common.service.RpcContext;
import us.kbase.common.service.UObject;
import workspace.CreateWorkspaceParams;
import workspace.ProvenanceAction;
import workspace.WorkspaceClient;
import workspace.WorkspaceIdentity;

public class KBaseRelationEngineServerTest {
    
    // there's no actual tests in here
    
    private static AuthToken token = null;
    private static Map<String, String> config = null;
    private static WorkspaceClient wsClient = null;
    private static String wsName = null;
    @SuppressWarnings("unused")
    private static KBaseSearchEngineServer impl = null;
    @SuppressWarnings("unused")
    private static Path scratch;
    @SuppressWarnings("unused")
    private static URL callbackURL;
    
    @BeforeClass
    public static void init() throws Exception {
        // Config loading
        String configFilePath = System.getenv("KB_DEPLOYMENT_CONFIG");
        if (configFilePath == null) {
            configFilePath = System.getProperty("KB_DEPLOYMENT_CONFIG");
        }
        File deploy = new File(configFilePath);
        Ini ini = new Ini(deploy);
        config = ini.get("KBaseSearchEngine");
        // Token validation
        String authUrl = config.get("auth-service-url");
        String authUrlInsecure = config.get("auth-service-url-allow-insecure");
        ConfigurableAuthService authService = new ConfigurableAuthService(
                new AuthConfig().withKBaseAuthServerURL(new URL(authUrl))
                .withAllowInsecureURLs("true".equals(authUrlInsecure)));
        String tokenString = System.getenv("KB_AUTH_TOKEN");
        if (tokenString == null) {
            tokenString = System.getProperty("KB_AUTH_TOKEN");
        }
        token = authService.validateToken(tokenString);
        // Reading URLs from config
        wsClient = new WorkspaceClient(new URL(config.get("workspace-url")), token);
        wsClient.setIsInsecureHttpConnectionAllowed(true); // do we need this?
        callbackURL = new URL(System.getenv("SDK_CALLBACK_URL"));
        scratch = Paths.get(config.get("scratch"));
        // These lines are necessary because we don't want to start linux syslog bridge service
        JsonServerSyslog.setStaticUseSyslog(false);
        JsonServerSyslog.setStaticMlogFile(new File(config.get("scratch"), "test.log")
            .getAbsolutePath());
        impl = new KBaseSearchEngineServer();
    }
    
    @SuppressWarnings("unused")
    private static String getWsName() throws Exception {
        if (wsName == null) {
            long suffix = System.currentTimeMillis();
            wsName = "test_KBaseSearchEngine_" + suffix;
            wsClient.createWorkspace(new CreateWorkspaceParams().withWorkspace(wsName));
        }
        return wsName;
    }
    
    @SuppressWarnings("unused")
    private static RpcContext getContext() {
        return new RpcContext().withProvenance(Arrays.asList(new ProvenanceAction()
            .withService("KBaseSearchEngine").withMethod("please_never_use_it_in_production")
            .withMethodParams(new ArrayList<UObject>())));
    }
    
    @AfterClass
    public static void cleanup() {
        if (wsName != null) {
            try {
                wsClient.deleteWorkspace(new WorkspaceIdentity().withWorkspace(wsName));
                System.out.println("Test workspace was deleted");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    
    @Test
    public void testYourMethod() throws Exception {
        //Map<String, Object> data = wsClient.getObjects2(new GetObjects2Params().withObjects(Arrays.asList(
        //        new ObjectSpecification().withRef("?/?/?")))).getData().get(0).getData().asClassInstance(Map.class);
        //System.out.println(data.keySet());
    }
}
