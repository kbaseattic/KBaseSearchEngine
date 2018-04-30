package kbasesearchengine.test.common;

import junit.framework.Assert;
import kbasesearchengine.common.GUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by apasha on 4/30/18.
 */
public class GUIDTest {
    final GUID guid1;
    final GUID guid2;
    final GUID guid3;
    final GUID guid4;
    final GUID guid5;
    final GUID guid6;
    final GUID guid7;
    final GUID guid8;


    public GUIDTest() {
        guid1 = new GUID("WS", 1, "1", 1,
                                      "contig", "NZ_MCBT01000008");
        guid2 = new GUID("WS", null, "1", null,
                null, null);
        guid3 = new GUID("WS", null, "1", 1,
                "contig", "NZ_MCBT01000008");
        guid4 = new GUID("WS", 1, "1", null,
                "contig", "NZ_MCBT01000008");
        guid5 = new GUID("WS", 1, "1", 1,
                null, null);
        guid6 = new GUID("WS", null, "1", null,
                "contig", "NZ_MCBT01000008");
        guid7 = new GUID("WS", null, "1", 1,
                null, null);
        guid8 = new GUID("WS", 1, "1", null,
                null, null);
    }


    @Before
    public void setup() throws Exception {
        new GUIDTest();
    }

    @After
    public void teardown() throws Exception {

    }

    @Test
    public void testToUUDI() {
        Assert.assertEquals("incorrect guid", 
                "f2912c05-4528-3492-938a-8e8c59525d4c", guid1.toUUID().toString());
        Assert.assertEquals("incorrect guid", 
                "ea2ebbef-e878-3593-b137-4aa40c37eb7a", guid2.toUUID().toString());
        Assert.assertEquals("incorrect guid", 
                "04fa4f7a-abfc-3b04-b505-834593bf097b", guid3.toUUID().toString());
        Assert.assertEquals("incorrect guid", 
                "01060998-9412-3168-8512-6581524f9a9c", guid4.toUUID().toString());
        Assert.assertEquals("incorrect guid", 
                "d4102da8-3782-3e20-88a5-05f822303b99", guid5.toUUID().toString());
        Assert.assertEquals("incorrect guid", 
                "03132c68-ec01-36cc-a3e4-eaea3301d8f4", guid6.toUUID().toString());
        Assert.assertEquals("incorrect guid", 
                "8d119b9e-91fc-3af7-811a-bf52e8edcdf7", guid7.toUUID().toString());
        Assert.assertEquals("incorrect guid", 
                "6c9bbd79-e10a-38f4-9828-eaf1373be71a", guid8.toUUID().toString());
    }
}
