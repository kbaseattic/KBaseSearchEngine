package kbasesearchengine.test.common;

import junit.framework.Assert;
import kbasesearchengine.common.GUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

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
    public void testGetURLEncoded() {

        try {
            Assert.assertEquals("incorrect guid",
                    "WS___1_1_1___contig_NZ_MCBT01000008", guid1.getURLEncoded());
            Assert.assertEquals("incorrect guid",
                    "WS___1", guid2.getURLEncoded());
            Assert.assertEquals("incorrect guid",
                    "WS___1_1___contig_NZ_MCBT01000008", guid3.getURLEncoded());
            Assert.assertEquals("incorrect guid",
                    "WS___1_1___contig_NZ_MCBT01000008", guid4.getURLEncoded());
            Assert.assertEquals("incorrect guid",
                    "WS___1_1_1", guid5.getURLEncoded());
            Assert.assertEquals("incorrect guid",
                    "WS___1___contig_NZ_MCBT01000008", guid6.getURLEncoded());
            Assert.assertEquals("incorrect guid",
                    "WS___1_1", guid7.getURLEncoded());
            Assert.assertEquals("incorrect guid",
                    "WS___1_1", guid8.getURLEncoded());
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testToString() {

        Assert.assertEquals("incorrect guid",
                "WS:1/1/1:contig/NZ_MCBT01000008", guid1.toString());
        Assert.assertEquals("incorrect guid",
                "WS:1", guid2.toString());
        Assert.assertEquals("incorrect guid",
                "WS:1/1:contig/NZ_MCBT01000008", guid3.toString());
        Assert.assertEquals("incorrect guid",
                "WS:1/1:contig/NZ_MCBT01000008", guid4.toString());
        Assert.assertEquals("incorrect guid",
                "WS:1/1/1", guid5.toString());
        Assert.assertEquals("incorrect guid",
                "WS:1:contig/NZ_MCBT01000008", guid6.toString());
        Assert.assertEquals("incorrect guid",
                "WS:1/1", guid7.toString());
        Assert.assertEquals("incorrect guid",
                "WS:1/1", guid8.toString());
    }

    @Test
    public void testLongGUIDException() {
        boolean exceptionCaught = false;
        try {
            StringBuffer bigSubObjId = new StringBuffer();
            for( int ii=0; ii<GUID.MAX_BYTES; ii++) {
                bigSubObjId.append('x');
            }

            new GUID("WS", 1, "1", null,
                    "contig", bigSubObjId.toString());

        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().contains("must be no longer than "+
                    GUID.MAX_BYTES+" bytes"));
            exceptionCaught = true;
        } finally {
          Assert.assertTrue(exceptionCaught);
        }
    }
}
