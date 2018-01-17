package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.ObjectTypeParsingRules;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.StorageObjectType;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class ObjectTypeParsingRulesTest {

    @Test
    public void equals() {
        EqualsVerifier.forClass(ObjectTypeParsingRules.class).usingGetClass().verify();
    }
    
    @Test
    public void buildWithoutIndexingRule() {
        final ObjectTypeParsingRules r = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1), new StorageObjectType("bar", "baz"))
                .build();
        
        assertThat("incorrect global type", r.getGlobalObjectType(),
                is(new SearchObjectType("foo", 1)));
        assertThat("incorrect UI name", r.getUiTypeName(), is("Foo"));
        assertThat("incorrect storage type", r.getStorageObjectType(),
                is(new StorageObjectType("bar", "baz")));
        assertThat("incorrect index rules", r.getIndexingRules(), is(Collections.emptyList()));
        assertThat("incorrect subobj type", r.getSubObjectType(), is(Optional.absent()));
        assertThat("incorrect subobj path", r.getSubObjectPath(), is(Optional.absent()));
        assertThat("incorrect subobj id path", r.getSubObjectIDPath(), is(Optional.absent()));
    }
    
    @Test
    public void buildMinimal() {
        final ObjectTypeParsingRules r = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("bar", "baz"))
                .withIndexingRule(IndexingRules.fromSourceKey("k", "n").build())
                .build();
        
        assertThat("incorrect global type", r.getGlobalObjectType(),
                is(new SearchObjectType("foo", 1)));
        assertThat("incorrect UI name", r.getUiTypeName(), is("Foo"));
        assertThat("incorrect storage type", r.getStorageObjectType(),
                is(new StorageObjectType("bar", "baz")));
        assertThat("incorrect index rules", r.getIndexingRules(), is(Arrays.asList(
                IndexingRules.fromSourceKey("k", "n").build())));
        assertThat("incorrect subobj type", r.getSubObjectType(), is(Optional.absent()));
        assertThat("incorrect subobj path", r.getSubObjectPath(), is(Optional.absent()));
        assertThat("incorrect subobj id path", r.getSubObjectIDPath(), is(Optional.absent()));
    }
    
    @Test
    public void buildWithUIName() {
        final ObjectTypeParsingRules r = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("bar", "baz"))
                .withIndexingRule(IndexingRules.fromSourceKey("k", "n").build())
                .withNullableUITypeName("whee whoo")
                .build();
        
        assertThat("incorrect global type", r.getGlobalObjectType(),
                is(new SearchObjectType("foo", 1)));
        assertThat("incorrect UI name", r.getUiTypeName(), is("whee whoo"));
        assertThat("incorrect storage type", r.getStorageObjectType(),
                is(new StorageObjectType("bar", "baz")));
        assertThat("incorrect index rules", r.getIndexingRules(), is(Arrays.asList(
                IndexingRules.fromSourceKey("k", "n").build())));
        assertThat("incorrect subobj type", r.getSubObjectType(), is(Optional.absent()));
        assertThat("incorrect subobj path", r.getSubObjectPath(), is(Optional.absent()));
        assertThat("incorrect subobj id path", r.getSubObjectIDPath(), is(Optional.absent()));
    }
    
    @Test
    public void buildSubObjectRules() throws Exception {
        // also tests indexing rule with from parent happy path
        final ObjectTypeParsingRules r = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("bar", "baz"))
                .withIndexingRule(IndexingRules.fromSourceKey("k", "n").build())
                .toSubObjectRule("sotype", new ObjectJsonPath("path"),
                        new ObjectJsonPath("idpath"))
                .withIndexingRule(IndexingRules.fromPath(new ObjectJsonPath("wugga"))
                        .withFromParent().build())
                .build();
        
        assertThat("incorrect global type", r.getGlobalObjectType(),
                is(new SearchObjectType("foo", 1)));
        assertThat("incorrect UI name", r.getUiTypeName(), is("Foo"));
        assertThat("incorrect storage type", r.getStorageObjectType(),
                is(new StorageObjectType("bar", "baz")));
        assertThat("incorrect index rules", r.getIndexingRules(), is(Arrays.asList(
                IndexingRules.fromSourceKey("k", "n").build(),
                IndexingRules.fromPath(new ObjectJsonPath("wugga"))
                        .withFromParent().build())));
        assertThat("incorrect subobj type", r.getSubObjectType(), is(Optional.of("sotype")));
        assertThat("incorrect subobj path", r.getSubObjectPath(),
                is(Optional.of(new ObjectJsonPath("path"))));
        assertThat("incorrect subobj id path", r.getSubObjectIDPath(),
                is(Optional.of(new ObjectJsonPath("idpath"))));
    }
    
    @Test
    public void buildWithUINameNullOrWhitespace() {
        buildWithUITypeNameNullOrWhitespace(null);
        buildWithUITypeNameNullOrWhitespace("  \t   \n  ");
    }

    private void buildWithUITypeNameNullOrWhitespace(final String uiTypeName) {
        final ObjectTypeParsingRules r = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("bar", "baz"))
                .withNullableUITypeName(uiTypeName)
                .withIndexingRule(IndexingRules.fromSourceKey("k", "n").build())
                .build();
        
        assertThat("incorrect global type", r.getGlobalObjectType(),
                is(new SearchObjectType("foo", 1)));
        assertThat("incorrect UI name", r.getUiTypeName(), is("Foo"));
        assertThat("incorrect storage type", r.getStorageObjectType(),
                is(new StorageObjectType("bar", "baz")));
        assertThat("incorrect index rules", r.getIndexingRules(), is(Arrays.asList(
                IndexingRules.fromSourceKey("k", "n").build())));
        assertThat("incorrect subobj type", r.getSubObjectType(), is(Optional.absent()));
        assertThat("incorrect subobj path", r.getSubObjectPath(), is(Optional.absent()));
        assertThat("incorrect subobj id path", r.getSubObjectIDPath(), is(Optional.absent()));
    }
    
    @Test
    public void builderSize() {
        final ObjectTypeParsingRules.Builder b = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("bar", "baz"));
        
        assertThat("incorrect size", b.numberOfIndexingRules(), is(0));
        
        b.withIndexingRule(IndexingRules.fromSourceKey("k", "n").build());
        assertThat("incorrect size", b.numberOfIndexingRules(), is(1));
        
        b.withIndexingRule(IndexingRules.fromSourceKey("l", "o").build());
        assertThat("incorrect size", b.numberOfIndexingRules(), is(2));
    }
    
    @Test
    public void getBuilderFail() {
        failGetBuilder(null, new StorageObjectType("c", "t"),
                new NullPointerException("globalObjectType"));
        failGetBuilder(new SearchObjectType("t", 1), null,
                new NullPointerException("storageType"));
    }
    
    private void failGetBuilder(
            final SearchObjectType globalObjectType,
            final StorageObjectType storageType,
            final Exception expected) {
        try {
            ObjectTypeParsingRules.getBuilder(globalObjectType, storageType);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void withIndexingRuleFail() {
        failWithIndexingRule(null, new NullPointerException("rules"));
        failWithIndexingRule(IndexingRules.fromSourceKey("k", "n").withFromParent().build(),
                new IllegalArgumentException("Cannot supply an indexing rule that extracts data " +
                        "from a parent to a rule set that applies to the parent"));
    }
    
    private void failWithIndexingRule(final IndexingRules rule, final Exception expected) {
        try {
            ObjectTypeParsingRules.getBuilder(
                    new SearchObjectType("t", 1),
                    new StorageObjectType("c", "t"))
                    .withIndexingRule(rule);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void toSubObjectRuleFail() throws Exception {
        final ObjectJsonPath p = new ObjectJsonPath("foo");
        failToSubObjectRule(null, p, p, new IllegalArgumentException(
                "subObjectType cannot be null or whitespace"));
        failToSubObjectRule("   \t   \n  ", p, p, new IllegalArgumentException(
                "subObjectType cannot be null or whitespace"));
        failToSubObjectRule("t", null, p, new NullPointerException("subObjectPath"));
        failToSubObjectRule("t", p, null, new NullPointerException("subObjectIDPath"));
    }
    
    private void failToSubObjectRule(
            final String subObjectType,
            final ObjectJsonPath subObjectPath,
            final ObjectJsonPath subObjectIDPath,
            final Exception expected) {
        try {
            ObjectTypeParsingRules.getBuilder(
                    new SearchObjectType("t", 1),
                    new StorageObjectType("c", "t"))
                    .toSubObjectRule(subObjectType, subObjectPath, subObjectIDPath);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void immutable() {
        final ObjectTypeParsingRules r = ObjectTypeParsingRules.getBuilder(
                new SearchObjectType("foo", 1),
                new StorageObjectType("bar", "baz"))
                .withIndexingRule(IndexingRules.fromSourceKey("k", "n").build())
                .build();
        
        try {
            r.getIndexingRules().add(IndexingRules.fromSourceKey("k", "n").build());
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new UnsupportedOperationException());
        }
    }
    
}
