package kbasesearchengine.test.common;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.bson.Document;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoDatabase;

import us.kbase.common.test.TestException;

public class TestCommon {
    
    public static final String TYPES_REPO_DIR = "resources/types";
    public static final String TYPE_MAP_REPO_DIR = "resources/typemappings";
    
    public static final String MONGOEXE = "test.mongo.exe";
    public static final String MONGOEXE_DEFAULT = "/opt/mongo/bin/mongod";
    public static final String MONGO_USE_WIRED_TIGER = "test.mongo.useWiredTiger";
    public static final String MONGO_USE_WIRED_TIGER_DEFAULT = "false";
    
    public static final String ELASTICEXE = "test.elasticsearch.exe";
    public static final String ELASTICEXE_DEFAULT = "/opt/elasticsearch/bin/elasticsearch";
    
    public static final String JARS_PATH = "test.jars.dir";
    public static final String JARS_PATH_DEFAULT = "/kb/deployment/lib/jars";
    public static final String WS_VER = "test.workspace.ver";
    public static final String WS_VER_DEFAULT = "0.8.0-dev3";
    
    public static final String AUTHSERV = "auth_service_url";
    public static final String TEST_TOKEN = "test_token";
    public static final String TEST_TOKEN2 = "test.token2";
    public static final String GLOBUS = "test.globus.url";
    public static final String GLOBUS_DEFAULT =
            "https://ci.kbase.us/services/auth/api/legacy/Globus";
    
    public static final String TEST_TEMP_DIR = "test.temp.dir";
    public static final String TEST_TEMP_DIR_DEFAULT = "/kb/module/work/tmp/testtmp";
    public static final String KEEP_TEMP_DIR = "test.temp.dir.keep";
    public static final String KEEP_TEMP_DIR_DEFAULT = "false";
    
    public static final String TEST_CONFIG_FILE_PROP_NAME = "test.cfg";
    public static final Path TEST_CONFIG_FILE_DEFAULT_PATH =
            Paths.get("/kb/module/work/test.cfg");
    
    private static Map<String, String> testConfig = null;
    private static Path testConfigFilePath = null;

    public static String getTestProperty(final String propertyKey) {
        return getTestProperty(propertyKey, null);
    }
    
    public static String getTestProperty(final String propertyKey, final String default_) {
        getTestConfig();
        final String prop = testConfig.get(propertyKey);
        if (prop == null || prop.trim().isEmpty()) {
            if (default_ != null) {
                System.out.println(String.format(
                        "Property %s of test file %s is missing, using default %s",
                        propertyKey, getConfigFilePath(), default_));
                return default_;
            }
            throw new TestException(String.format("Property %s of test file %s is missing",
                    propertyKey, getConfigFilePath()));
        }
        return prop;
    }

    private static void getTestConfig() {
        if (testConfig != null) {
            return;
        }
        final Path testCfgFilePath = getConfigFilePath();
        Properties p = new Properties();
        try {
            p.load(Files.newInputStream(testCfgFilePath));
        } catch (IOException ioe) {
            throw new TestException(String.format(
                    "IO Error reading the test configuration file %s: %s",
                    testCfgFilePath, ioe.getMessage()), ioe);
        }
        testConfig = new HashMap<>();
        for (final String s: p.stringPropertyNames()) {
            testConfig.put(s, p.getProperty(s));
        }
    }

    public static Path getConfigFilePath() {
        if (testConfigFilePath != null) {
            return testConfigFilePath;
        }
        String testCfgFilePathStr = System.getProperty(TEST_CONFIG_FILE_PROP_NAME);
        if (testCfgFilePathStr == null || testCfgFilePathStr.trim().isEmpty()) {
            System.out.println(String.format(
                    "No test config file specified in system property %s, using default of %s",
                    TEST_CONFIG_FILE_PROP_NAME, TEST_CONFIG_FILE_DEFAULT_PATH));
            testConfigFilePath = TEST_CONFIG_FILE_DEFAULT_PATH;
        } else {
            testConfigFilePath = Paths.get(testCfgFilePathStr).toAbsolutePath().normalize();
        }
        return testConfigFilePath;
    }
    
    public static void destroyDB(MongoDatabase db) {
        for (String name: db.listCollectionNames()) {
            if (!name.startsWith("system.")) {
                // dropping collection also drops indexes
                db.getCollection(name).deleteMany(new Document());
            }
        }
    }
    
    public static String getMongoExe() {
        return getTestProperty(MONGOEXE, MONGOEXE_DEFAULT);
    }
    
    public static boolean useWiredTigerEngine() {
        return "true".equals(
                getTestProperty(MONGO_USE_WIRED_TIGER, MONGO_USE_WIRED_TIGER_DEFAULT));
    }

    public static String getTempDir() {
        return getTestProperty(TEST_TEMP_DIR, TEST_TEMP_DIR_DEFAULT);
    }
    
    public static boolean getDeleteTempFiles() {
        return !"true".equals(getTestProperty(KEEP_TEMP_DIR, KEEP_TEMP_DIR_DEFAULT));
    }

    public static String getElasticSearchExe() {
        return getTestProperty(ELASTICEXE, ELASTICEXE_DEFAULT);
    }
    
    public static Path getJarsDir() {
        return Paths.get(getTestProperty(JARS_PATH, JARS_PATH_DEFAULT));
    }
    
    public static String getWorkspaceVersion() {
        return getTestProperty(WS_VER, WS_VER_DEFAULT);
    }
    
    public static void stfuLoggers() {
        java.util.logging.Logger.getLogger("com.mongodb")
                .setLevel(java.util.logging.Level.OFF);
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME))
                .setLevel(ch.qos.logback.classic.Level.OFF);
    }
    
    public static void assertExceptionCorrect(
            final Exception got,
            final Exception expected) {
        final StringWriter sw = new StringWriter();
        got.printStackTrace(new PrintWriter(sw));
        assertThat("incorrect exception. trace:\n" +
                sw.toString(),
                got.getLocalizedMessage(),
                is(expected.getLocalizedMessage()));
        assertThat("incorrect exception type", got, instanceOf(expected.getClass()));
    }
    
    @SafeVarargs
    public static <T> Set<T> set(T... objects) {
        return new HashSet<T>(Arrays.asList(objects));
    }
    
    public static void assertCloseMS(
            final Instant start,
            final Instant end,
            final int differenceMS,
            final int slopMS) {
        final long gotDiff = end.toEpochMilli() - start.toEpochMilli();
        assertThat(String.format("time difference not within bounds: %s %s %s %s %s",
                start, end, gotDiff, differenceMS, slopMS),
                Math.abs(gotDiff - differenceMS) < slopMS, is(true));
    }

    public static void createAuthUser(
            final URL authURL,
            final String userName,
            final String displayName)
            throws Exception {
        final URL target = new URL(authURL.toString() + "/api/V2/testmodeonly/user");
        final HttpURLConnection conn = (HttpURLConnection) target.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("content-type", "application/json");
        conn.setDoOutput(true);
        
        final DataOutputStream writer = new DataOutputStream(conn.getOutputStream());
        writer.writeBytes(new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "user", userName,
                "display", displayName)));
        writer.flush();
        writer.close();
        
        if (conn.getResponseCode() != 200) {
            final String err = IOUtils.toString(conn.getErrorStream()); 
            System.out.println(err);
            throw new TestException(err.substring(1, 200));
        }
    }

    public static String createLoginToken(final URL authURL, String user) throws Exception {
        final URL target = new URL(authURL.toString() + "/api/V2/testmodeonly/token");
        final HttpURLConnection conn = (HttpURLConnection) target.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("content-type", "application/json");
        conn.setDoOutput(true);
        
        final DataOutputStream writer = new DataOutputStream(conn.getOutputStream());
        writer.writeBytes(new ObjectMapper().writeValueAsString(ImmutableMap.of(
                "user", user,
                "type", "Login")));
        writer.flush();
        writer.close();
        
        if (conn.getResponseCode() != 200) {
            final String err = IOUtils.toString(conn.getErrorStream()); 
            System.out.println(err);
            throw new TestException(err.substring(1, 200));
        }
        final String out = IOUtils.toString(conn.getInputStream());
        @SuppressWarnings("unchecked")
        final Map<String, Object> resp = new ObjectMapper().readValue(out, Map.class);
        return (String) resp.get("token");
    }
}
