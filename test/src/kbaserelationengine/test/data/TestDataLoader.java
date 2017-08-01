package kbaserelationengine.test.data;

import java.io.InputStream;
import java.util.Scanner;

import us.kbase.common.test.TestException;

public class TestDataLoader {

    public static void main(String[] args) {
        System.out.println(load("NarrativeObject1"));

    }
    
    public static String load(final String filename) {
        final InputStream is = TestDataLoader.class.getResourceAsStream(filename);
        if (is == null) {
            throw new TestException("Can't open file " + filename);
        } else {
            final Scanner s = new Scanner(is);
            s.useDelimiter("\\A");
            final String commit = s.hasNext() ? s.next() : "";
            s.close();
            return commit;
        }
    }

}
