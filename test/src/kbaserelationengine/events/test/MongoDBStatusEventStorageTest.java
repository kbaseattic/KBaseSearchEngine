package kbaserelationengine.events.test;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import kbaserelationengine.events.AccessGroupStatus;
import kbaserelationengine.events.MongoDBStatusEventStorage;

public class MongoDBStatusEventStorageTest {
	MongoDBStatusEventStorage mdStorage;

	@Before
	public void init(){
        mdStorage  = new MongoDBStatusEventStorage("localhost", 27017);
	}
	
	//@Test
	public void test01() throws IOException {
		AccessGroupStatus gs;
		
		gs = new AccessGroupStatus(null, "WS", 10, 123L, new String[]{"u1", "u2"});
		mdStorage.store(gs);

		gs = new AccessGroupStatus(null, "WS", 11, 123L, new String[]{"u1", "u50"});
		mdStorage.store(gs);
	}
	
	@Test
	public void test02() throws IOException {
		System.out.println(  mdStorage.findAccessGroupIds("WS", "rsuotrmin") );
	}
	

}
