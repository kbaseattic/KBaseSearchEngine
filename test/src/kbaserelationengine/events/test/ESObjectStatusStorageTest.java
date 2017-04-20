package kbaserelationengine.events.test;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.junit.Test;

import kbaserelationengine.events.ESObjectStatusStorage;

public class ESObjectStatusStorageTest {

	@Test
	public void test01() throws IOException {
		ESObjectStatusStorage storage = new ESObjectStatusStorage(new HttpHost("localhost", 9200));
		storage.deleteStorage();
		storage.createStorage();
		
//		ObjectStatus objStat = new ObjectStatus(null, "WS", 15, "1", 2, "KBase.Genome",ObjectStatusEventType.CREATED);
//		storage.statusChanged(objStat);
	
	}

}
