package kbasesearchengine.events.storage;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.events.AccessGroupStatus;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.test.controllers.mongo.MongoController;

public class MongoDBStatusEventStorageTest {

    private MongoDBStatusEventStorage mdStorage;
    private static MongoController mongo;
    private static MongoDatabase db;
    private static MongoClient mc;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestCommon.stfuLoggers();
        mongo = new MongoController(
                TestCommon.getMongoExe(),
                Paths.get(TestCommon.getTempDir()),
                TestCommon.useWiredTigerEngine());
        mc = new MongoClient("localhost:" + mongo.getServerPort());
        db = mc.getDatabase("DataStatus");
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        if (mc != null) {
            mc.close();
        }
        if (mongo != null) {
            mongo.destroy(TestCommon.getDeleteTempFiles());
        }
    }
    
    @Before
    public void init() throws Exception {
        TestCommon.destroyDB(db);
        mdStorage  = new MongoDBStatusEventStorage(db);
    }

    //@Test
    public void test01() throws IOException {
        AccessGroupStatus gs;

        gs = new AccessGroupStatus(null, "WS", 10, 123L,false,false, Arrays.asList("u1", "u2"));
        mdStorage.store(gs);

        gs = new AccessGroupStatus(null, "WS", 11, 123L, false,false, Arrays.asList("u1", "u50"));
        mdStorage.store(gs);
    }

    @Test
    public void test02() throws IOException {
        System.out.println(  mdStorage.findAccessGroupIds("psnovichkov") );
    }


}
