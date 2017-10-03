package kbasesearchengine.queue.test;

import java.io.IOException;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.queue.ObjectStatusEventIterator;
import kbasesearchengine.queue.ObjectStatusEventQueue;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.test.controllers.mongo.MongoController;

public class ObjectStatusEventQueueTest {

    private ObjectStatusEventQueue queue;
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
    public void init(){
        TestCommon.destroyDB(db);
        //ESObjectStatusEventStorage storage = new ESObjectStatusEventStorage(new HttpHost("localhost", 9200));
        MongoDBStatusEventStorage storage = new MongoDBStatusEventStorage(db);

        queue = new ObjectStatusEventQueue(storage);
    }

    @Test
    public void testIterator() throws Exception {
        int count = queue.count();
        System.out.println("Number of records " + count);


        ObjectStatusEventIterator it = queue.iterator("WS");
        int i = 0;
        int n = 0;
        while(it.hasNext()){
            it.next();
            //			System.out.println(it.next());
            if(i%2 == 0){
                it.markAsVisited(false);
                n++;
            }
            i++;
        }

        count = queue.count();
        System.out.println("Number of records " + count);		
        System.out.println("Number of marked " + n);		

    }

    //@Test
    public void testUnmarkDataType() throws IOException {

        System.out.println("Number of records:");
        System.out.println("\tbefore\t" + queue.count());	

        queue.markAsNonprocessed("WS", "KBaseNarrative.Narrative");

        System.out.println("\tafter\t" + queue.count());		
    }
}
