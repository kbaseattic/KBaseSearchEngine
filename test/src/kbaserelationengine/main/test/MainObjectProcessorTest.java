package kbaserelationengine.main.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.junit.Test;

import kbaserelationengine.main.MainObjectProcessor;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;

public class MainObjectProcessorTest {

    @Test
    public void testManual() throws Exception {
        File testCfg = new File("test_local/test.cfg");
        Properties props = new Properties();
        try (InputStream is = new FileInputStream(testCfg)) {
            props.load(is);
        }
        URL authUrl = new URL(props.getProperty("auth_service_url"));
        String authAllowInsecure = props.getProperty("auth_service_url_allow_insecure");
        ConfigurableAuthService authSrv = new ConfigurableAuthService(
                new AuthConfig().withKBaseAuthServerURL(authUrl)
                .withAllowInsecureURLs("true".equals(authAllowInsecure)));
        String tokenStr = props.getProperty("secure.indexer_token");
        AuthToken kbaseIndexerToken = authSrv.validateToken(tokenStr);
        String mongoHost = props.getProperty("secure.mongo_host");
        int mongoPort = Integer.parseInt(props.getProperty("secure.mongo_port"));
        String elasticHost = props.getProperty("secure.elastic_host");
        int elasticPort = Integer.parseInt(props.getProperty("secure.elastic_port"));
        HttpHost esHostPort = new HttpHost(elasticHost, elasticPort);
        String kbaseEndpoint = props.getProperty("kbase_endpoint");
        URL wsUrl = new URL(kbaseEndpoint + "/ws");
        File typesDir = new File("resources/types");
        File tempDir = new File("test_local/temp_files");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        String mongoDbName = "test_" + System.currentTimeMillis() + "_DataStatus";
        String esIndexPrefix = "test_" + System.currentTimeMillis() + ".";
        MainObjectProcessor mop = new MainObjectProcessor(wsUrl, kbaseIndexerToken, mongoHost,
                mongoPort, mongoDbName, esHostPort, esIndexPrefix, typesDir, tempDir, false);
        mop.performOneTick();
    }
}
