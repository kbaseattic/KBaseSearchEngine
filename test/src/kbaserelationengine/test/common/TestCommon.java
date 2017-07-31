package kbaserelationengine.test.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.bson.Document;

import com.mongodb.client.MongoDatabase;

import us.kbase.common.test.TestException;

public class TestCommon {
    
    public static final String MONGOEXE = "test.mongo.exe";
    public static final String MONGOEXE_DEFAULT = "/opt/mongo/bin/mongod";
    public static final String MONGO_USE_WIRED_TIGER = "test.mongo.useWiredTiger";
    public static final String MONGO_USE_WIRED_TIGER_DEFAULT = "false";
    
    public static final String ELASTICEXE = "test.elasticsearch.exe";
    public static final String ELASTICEXE_DEFAULT = "/opt/elasticsearch/bin/elasticsearch";
    
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
    
}
