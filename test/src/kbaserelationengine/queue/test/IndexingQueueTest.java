package kbaserelationengine.queue.test;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.ESObjectStatusStorage;
import kbaserelationengine.queue.IndexingIterator;
import kbaserelationengine.queue.IndexingQueue;

public class IndexingQueueTest {
	IndexingQueue queue;
	
	@Before
	public void init(){
		ESObjectStatusStorage storage = new ESObjectStatusStorage(new HttpHost("localhost", 9200));
		queue = new IndexingQueue(storage);
	}
	
	@Test
	public void testIterator() throws IOException {
		int count = queue.count();
		System.out.println("Number of records " + count);

		
		IndexingIterator it = queue.iterator("WS");
		int i = 0;
		while(it.hasNext()){
			System.out.println(it.next());
//			if(i++%5 == 0){
				it.markAsVisitied(true);
//			}
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
