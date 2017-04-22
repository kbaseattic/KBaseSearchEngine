package kbaserelationengine.queue.test;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.ESObjectStatusEventStorage;
import kbaserelationengine.events.MongoDBObjectStatusEventStorage;
import kbaserelationengine.queue.ObjectStatusEventIterator;
import kbaserelationengine.queue.ObjectStatusEventQueue;

public class ObjectStatusEventQueueTest {
	ObjectStatusEventQueue queue;
	
	@Before
	public void init(){
//		ESObjectStatusEventStorage storage = new ESObjectStatusEventStorage(new HttpHost("localhost", 9200));
		MongoDBObjectStatusEventStorage storage = new MongoDBObjectStatusEventStorage("localhost", 27017);

		queue = new ObjectStatusEventQueue(storage);
	}
	
	@Test
	public void testIterator() throws IOException {
		int count = queue.count();
		System.out.println("Number of records " + count);

		
		ObjectStatusEventIterator it = queue.iterator("WS");
		int i = 0;
		while(it.hasNext()){
			System.out.println(it.next());
			if(i++%5 == 0){
				it.markAsVisitied(true);
			}
		}
		
		count = queue.count();
		System.out.println("Number of records " + count);		
		
	}
	
	@Test
	public void testUnmarkDataType() throws IOException {
		
		System.out.println("Number of records:");
		System.out.println("\tbefore\t" + queue.count());	
		
		queue.markAsNonprocessed("WS", "KBaseBiochem.Media");
		
		System.out.println("\tafter\t" + queue.count());		
	}
}
