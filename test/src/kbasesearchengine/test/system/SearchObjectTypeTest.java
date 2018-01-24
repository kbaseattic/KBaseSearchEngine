package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class SearchObjectTypeTest {

    @Test
    public void equals() {
        EqualsVerifier.forClass(SearchObjectType.class).usingGetClass().verify();
    }
    
    @Test
    public void construct() {
        final SearchObjectType type = new SearchObjectType("   \t   fo9O  \n ", 1);
        
        assertThat("incorrect type", type.getType(), is("fo9O"));
        assertThat("incorrect version", type.getVersion(), is(1));
        assertThat("incorrect toString", type.toString(),
                is("SearchObjectType [type=fo9O, version=1]"));
    }
    
    @Test
    public void constructFail() {
        failConstruct(null, 1, new IllegalArgumentException(
                "search type cannot be null or whitespace"));
        failConstruct("   \t \n  ", 1, new IllegalArgumentException(
                "search type cannot be null or whitespace"));
        failConstruct("t", 0, new IllegalArgumentException(
                "search type version must be greater than zero"));

        char [] long_name = new char[SearchObjectType.MAX_TYPE_SIZE+1];
        for(int ii=0; ii<SearchObjectType.MAX_TYPE_SIZE+1; ii++) {
            long_name[ii] = 'a';
        }
        failConstruct(new String(long_name),
                1, new IllegalArgumentException(
                "Search type string size must be less than " +
                        SearchObjectType.MAX_TYPE_SIZE + " UTF-8 chars"));
    }
    
    @Test
    public void constructFailIllegalName() {
        failConstruct("   foo_bar  ", "Illegal character in search type name foo_bar: _");
        failConstruct("   foo\u0980bar  ",
                "Illegal character in search type name foo\u0980bar: \u0980");
        failConstruct("   foo*bar  ", "Illegal character in search type name foo*bar: *");
        
        failConstruct("   7oobar  ", "Search type name 7oobar must start with a letter");
    }

    private void failConstruct(final String type, final String exceptionString) {
        failConstruct(type, 1, new IllegalArgumentException(exceptionString));
    }
    
    private void failConstruct(final String type, final int ver, final Exception expected) {
        try {
            new SearchObjectType(type, ver);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
}
