package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.ParsingRulesSubtypeFirstComparator;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;

public class ParsingRulesSubtypeFirstComparatorTest {
    
    private static final ObjectJsonPath foo;
    private static final ObjectJsonPath bar;
    static {
        try {
            foo = new ObjectJsonPath("/foo");
            bar = new ObjectJsonPath("/bar");
        } catch (Exception e) {
            throw new RuntimeException("tests are broken", e);
        }
    }
    
    private static final ObjectTypeParsingRules R1 = ObjectTypeParsingRules.getBuilder(
            new SearchObjectType("foo1", 1), new StorageObjectType("Ws", "Genome"))
            .build();
    private static final ObjectTypeParsingRules R2 = ObjectTypeParsingRules.getBuilder(
            new SearchObjectType("foo2sub", 1), new StorageObjectType("Ws", "Genome"))
            .toSubObjectRule("foosub", foo, bar)
            .build();
    private static final ObjectTypeParsingRules R3 = ObjectTypeParsingRules.getBuilder(
            new SearchObjectType("foo3", 1), new StorageObjectType("Ws", "Genome"))
            .build();
    private static final ObjectTypeParsingRules R4 = ObjectTypeParsingRules.getBuilder(
            new SearchObjectType("foo4sub", 1), new StorageObjectType("Ws", "Genome"))
            .toSubObjectRule("foosub", foo, bar)
            .build();

    @Test
    public void compare() {
        assertThat("incorrect comparison",
                new ParsingRulesSubtypeFirstComparator().compare(R1, R3), is(0));
        assertThat("incorrect comparison",
                new ParsingRulesSubtypeFirstComparator().compare(R2, R4), is(0));
        assertThat("incorrect comparison",
                new ParsingRulesSubtypeFirstComparator().compare(R1, R2), is(1));
        assertThat("incorrect comparison",
                new ParsingRulesSubtypeFirstComparator().compare(R2, R1), is(-1));
        
    }
    
    @Test
    public void sortWithList() throws Exception {
        final List<ObjectTypeParsingRules> rules = new ArrayList<>(
                Arrays.asList(R1, R2, R3, R4));
        Collections.sort(rules, new ParsingRulesSubtypeFirstComparator());
        
        assertThat("incorrect sort", rules, is(Arrays.asList(R2, R4, R1, R3)));
    }
    

}
