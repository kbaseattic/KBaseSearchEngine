package kbasesearchengine.events.storage;

import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.events.storage.OldMongoDBStatusEventStorage;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.test.controllers.mongo.MongoController;

public class MongoDBStatusEventStorageTest {

    @SuppressWarnings("unused")
    private OldMongoDBStatusEventStorage mdStorage;
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
        mdStorage  = new OldMongoDBStatusEventStorage(db);
    }


    @Test
    public void fakeTestNeedToAddSome() {}
    
}
