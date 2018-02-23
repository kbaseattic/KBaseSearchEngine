package kbasesearchengine.test.search;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.search.SortingRule;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class SortingRuleTest {
    
    @Test
    public void equals() {
        EqualsVerifier.forClass(SortingRule.class).usingGetClass().verify();
    }
    
    @Test
    public void buildKeyPropMinimal() {
        final SortingRule sr = SortingRule.getKeyPropertyBuilder("objectfield").build();
        
        assertThat("incorrect key prop", sr.getKeyProperty(), is(Optional.of("objectfield")));
        assertThat("incorrect std prop", sr.getStandardProperty(), is(Optional.absent()));
        assertThat("incorrect is asc", sr.isAscending(), is(true));
        assertThat("incorrect is key prop", sr.isKeyProperty(), is(true));
    }
    
    @Test
    public void buildKeyPropNullAsc() {
        final SortingRule sr = SortingRule.getKeyPropertyBuilder("objectfield")
                .withNullableIsAscending(null).build();
        
        assertThat("incorrect key prop", sr.getKeyProperty(), is(Optional.of("objectfield")));
        assertThat("incorrect std prop", sr.getStandardProperty(), is(Optional.absent()));
        assertThat("incorrect is asc", sr.isAscending(), is(true));
        assertThat("incorrect is key prop", sr.isKeyProperty(), is(true));
    }
    
    @Test
    public void buildKeyPropDesc() {
        final SortingRule sr = SortingRule.getKeyPropertyBuilder("objectfield")
                .withNullableIsAscending(false).build();
        
        assertThat("incorrect key prop", sr.getKeyProperty(), is(Optional.of("objectfield")));
        assertThat("incorrect std prop", sr.getStandardProperty(), is(Optional.absent()));
        assertThat("incorrect is asc", sr.isAscending(), is(false));
        assertThat("incorrect is key prop", sr.isKeyProperty(), is(true));
    }
    
    @Test
    public void buildStdPropMinimal() {
        final SortingRule sr = SortingRule.getStandardPropertyBuilder("timestamp").build();
        
        assertThat("incorrect key prop", sr.getKeyProperty(), is(Optional.absent()));
        assertThat("incorrect std prop", sr.getStandardProperty(), is(Optional.of("timestamp")));
        assertThat("incorrect is asc", sr.isAscending(), is(true));
        assertThat("incorrect is key prop", sr.isKeyProperty(), is(false));
    }
    
    @Test
    public void buildStdPropNullAsc() {
        final SortingRule sr = SortingRule.getStandardPropertyBuilder("timestamp")
                .withNullableIsAscending(null).build();
        
        assertThat("incorrect key prop", sr.getKeyProperty(), is(Optional.absent()));
        assertThat("incorrect std prop", sr.getStandardProperty(), is(Optional.of("timestamp")));
        assertThat("incorrect is asc", sr.isAscending(), is(true));
        assertThat("incorrect is key prop", sr.isKeyProperty(), is(false));
    }
    
    @Test
    public void buildStdPropDesc() {
        final SortingRule sr = SortingRule.getStandardPropertyBuilder("timestamp")
                .withNullableIsAscending(false).build();
        
        assertThat("incorrect key prop", sr.getKeyProperty(), is(Optional.absent()));
        assertThat("incorrect std prop", sr.getStandardProperty(), is(Optional.of("timestamp")));
        assertThat("incorrect is asc", sr.isAscending(), is(false));
        assertThat("incorrect is key prop", sr.isKeyProperty(), is(false));
    }
    
    @Test
    public void buildFailStandard() {
        failBuildStandard(null, new IllegalArgumentException(
                "The key property or standard property cannot be null or whitespace only"));
        failBuildStandard("   \t   \n  ", new IllegalArgumentException(
                "The key property or standard property cannot be null or whitespace only"));
    }
    
    private void failBuildStandard(final String stdkey, final Exception expected) {
        try {
            SortingRule.getStandardPropertyBuilder(stdkey);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void buildFailKeyProp() {
        failBuildKeyProp(null, new IllegalArgumentException(
                "The key property or standard property cannot be null or whitespace only"));
        failBuildKeyProp("   \t   \n  ", new IllegalArgumentException(
                "The key property or standard property cannot be null or whitespace only"));
    }
    
    private void failBuildKeyProp(final String keyprop, final Exception expected) {
        try {
            SortingRule.getKeyPropertyBuilder(keyprop);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

}
