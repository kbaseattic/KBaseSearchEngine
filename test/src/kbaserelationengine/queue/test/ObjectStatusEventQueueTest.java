package kbaserelationengine.queue.test;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.storage.MongoDBStatusEventStorage;
import kbaserelationengine.queue.ObjectStatusEventIterator;
import kbaserelationengine.queue.ObjectStatusEventQueue;

public class ObjectStatusEventQueueTest {
	ObjectStatusEventQueue queue;
	
	//TODO NOW Automate tests mg
	
	@Before
	public void init(){
//		ESObjectStatusEventStorage storage = new ESObjectStatusEventStorage(new HttpHost("localhost", 9200));
		MongoDBStatusEventStorage storage = new MongoDBStatusEventStorage("localhost", 27017);

		queue = new ObjectStatusEventQueue(storage);
	}
	
	@Test
	public void testIterator() throws IOException {
		int count = queue.count();
		System.out.println("Number of records " + count);

		
		ObjectStatusEventIterator it = queue.iterator("WS");
		int i = 0;
		int n = 0;
		while(it.hasNext()){
			it.next();
//			System.out.println(it.next());
			if(i%2 == 0){
				it.markAsVisitied(false);
				n++;
			}
			i++;
		}
		
		count = queue.count();
		System.out.println("Number of records " + count);		
		System.out.println("Number of marked " + n);		
		
	}
	
//	@Test
	public void testUnmarkDataType() throws IOException {
		
		System.out.println("Number of records:");
		System.out.println("\tbefore\t" + queue.count());	
		
		queue.markAsNonprocessed("WS", "KBaseNarrative.Narrative");
		
		System.out.println("\tafter\t" + queue.count());		
	}
}
