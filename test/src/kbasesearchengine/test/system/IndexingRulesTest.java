package kbasesearchengine.test.system;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.base.Optional;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.system.IndexingRules;
import kbasesearchengine.system.IndexingRules.Builder;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.system.Transform;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class IndexingRulesTest {
    
    @Test
    public void equals() {
        EqualsVerifier.forClass(IndexingRules.class).usingGetClass().verify();
    }
    
    @Test
    public void buildPathMinimal() throws Exception {
        final IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("foo")).build();
        assertThat("incorrect path", ir.getPath(), is(Optional.of(new ObjectJsonPath("foo"))));
        assertThat("incorrect default", ir.getDefaultValue(), is(Optional.absent()));
        assertThat("incorrect key name", ir.getKeyName(), is("foo"));
        assertThat("incorrect keyword type", ir.getKeywordType(), is(Optional.of("keyword")));
        assertThat("incorrect source key", ir.getSourceKey(), is(Optional.absent()));
        assertThat("incorrect transform", ir.getTransform(), is(Optional.absent()));
        assertThat("incorrect link key", ir.getUiLinkKey(), is(Optional.absent()));
        assertThat("incorrect ui name", ir.getUiName(), is("Foo"));
        assertThat("incorrect is derived", ir.isDerivedKey(), is(false));
        assertThat("incorrect is from parent", ir.isFromParent(), is(false));
        assertThat("incorrect is full text", ir.isFullText(), is(false));
        assertThat("incorrect is not indexed", ir.isNotIndexed(), is(false));
        assertThat("incorrect is ui hidden", ir.isUiHidden(), is(false));
    }
    
    @Test
    public void buildSourceKeyMinimal() throws Exception {
        final IndexingRules ir = IndexingRules.fromSourceKey("foo", "bar").build();
        assertThat("incorrect path", ir.getPath(), is(Optional.absent()));
        assertThat("incorrect default", ir.getDefaultValue(), is(Optional.absent()));
        assertThat("incorrect key name", ir.getKeyName(), is("bar"));
        assertThat("incorrect keyword type", ir.getKeywordType(), is(Optional.of("keyword")));
        assertThat("incorrect source key", ir.getSourceKey(), is(Optional.of("foo")));
        assertThat("incorrect transform", ir.getTransform(), is(Optional.absent()));
        assertThat("incorrect link key", ir.getUiLinkKey(), is(Optional.absent()));
        assertThat("incorrect ui name", ir.getUiName(), is("Bar"));
        assertThat("incorrect is derived", ir.isDerivedKey(), is(true));
        assertThat("incorrect is from parent", ir.isFromParent(), is(false));
        assertThat("incorrect is full text", ir.isFullText(), is(false));
        assertThat("incorrect is not indexed", ir.isNotIndexed(), is(false));
        assertThat("incorrect is ui hidden", ir.isUiHidden(), is(false));
    }
    
    @Test
    public void buildPathMaximalKeywordType() throws Exception {
        final IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("foo"))
                .withFromParent()
                .withFullText()
                .withKeyName("thunderpants")
                .withKeywordType("thingy")
                .withNotIndexed()
                .withNullableDefaultValue("default")
                .withNullableUILinkKey("linky")
                .withNullableUIName("myname")
                .withTransform(Transform.values())
                .withUIHidden()
                .build();
        assertThat("incorrect path", ir.getPath(), is(Optional.of(new ObjectJsonPath("foo"))));
        assertThat("incorrect default", ir.getDefaultValue(), is(Optional.of("default")));
        assertThat("incorrect key name", ir.getKeyName(), is("thunderpants"));
        assertThat("incorrect keyword type", ir.getKeywordType(), is(Optional.of("thingy")));
        assertThat("incorrect source key", ir.getSourceKey(), is(Optional.absent()));
        assertThat("incorrect transform", ir.getTransform(), is(Optional.of(Transform.values())));
        assertThat("incorrect link key", ir.getUiLinkKey(), is(Optional.of("linky")));
        assertThat("incorrect ui name", ir.getUiName(), is("myname"));
        assertThat("incorrect is derived", ir.isDerivedKey(), is(false));
        assertThat("incorrect is from parent", ir.isFromParent(), is(true));
        assertThat("incorrect is full text", ir.isFullText(), is(false));
        assertThat("incorrect is not indexed", ir.isNotIndexed(), is(true));
        assertThat("incorrect is ui hidden", ir.isUiHidden(), is(true));
    }

    @Test
    public void buildPathMaximalFullText() throws Exception {
        // also tests passing null for nullable default
        final IndexingRules ir = IndexingRules.fromPath(new ObjectJsonPath("foo"))
                .withFromParent()
                .withKeyName("thunderpants")
                .withKeywordType("thingy")
                .withFullText()
                .withNotIndexed()
                .withNullableDefaultValue(null)
                .withNullableUILinkKey("linky")
                .withNullableUIName("myname")
                .withTransform(Transform.values())
                .withUIHidden()
                .build();
        assertThat("incorrect path", ir.getPath(), is(Optional.of(new ObjectJsonPath("foo"))));
        assertThat("incorrect default", ir.getDefaultValue(), is(Optional.absent()));
        assertThat("incorrect key name", ir.getKeyName(), is("thunderpants"));
        assertThat("incorrect keyword type", ir.getKeywordType(), is(Optional.absent()));
        assertThat("incorrect source key", ir.getSourceKey(), is(Optional.absent()));
        assertThat("incorrect transform", ir.getTransform(), is(Optional.of(Transform.values())));
        assertThat("incorrect link key", ir.getUiLinkKey(), is(Optional.of("linky")));
        assertThat("incorrect ui name", ir.getUiName(), is("myname"));
        assertThat("incorrect is derived", ir.isDerivedKey(), is(false));
        assertThat("incorrect is from parent", ir.isFromParent(), is(true));
        assertThat("incorrect is full text", ir.isFullText(), is(true));
        assertThat("incorrect is not indexed", ir.isNotIndexed(), is(true));
        assertThat("incorrect is ui hidden", ir.isUiHidden(), is(true));
    }
    
    @Test
    public void buildSourceKeyMaximalKeywordType() throws Exception {
        final IndexingRules ir = IndexingRules.fromSourceKey("whoop", "tedoo")
                .withFromParent()
                .withFullText()
                .withKeyName("thunderpants")
                .withKeywordType("thingy")
                .withNotIndexed()
                .withNullableDefaultValue("default")
                .withNullableUILinkKey("linky")
                .withNullableUIName("myname")
                .withTransform(Transform.values())
                .withUIHidden()
                .build();
        assertThat("incorrect path", ir.getPath(), is(Optional.absent()));
        assertThat("incorrect default", ir.getDefaultValue(), is(Optional.of("default")));
        assertThat("incorrect key name", ir.getKeyName(), is("thunderpants"));
        assertThat("incorrect keyword type", ir.getKeywordType(), is(Optional.of("thingy")));
        assertThat("incorrect source key", ir.getSourceKey(), is(Optional.of("whoop")));
        assertThat("incorrect transform", ir.getTransform(), is(Optional.of(Transform.values())));
        assertThat("incorrect link key", ir.getUiLinkKey(), is(Optional.of("linky")));
        assertThat("incorrect ui name", ir.getUiName(), is("myname"));
        assertThat("incorrect is derived", ir.isDerivedKey(), is(true));
        assertThat("incorrect is from parent", ir.isFromParent(), is(true));
        assertThat("incorrect is full text", ir.isFullText(), is(false));
        assertThat("incorrect is not indexed", ir.isNotIndexed(), is(true));
        assertThat("incorrect is ui hidden", ir.isUiHidden(), is(true));
    }
    
    @Test
    public void buildSourceKeyMaximalFullText() throws Exception {
        final IndexingRules ir = IndexingRules.fromSourceKey("whoop", "tedoo")
                .withFromParent()
                .withKeyName("thunderpants")
                .withKeywordType("thingy")
                .withFullText()
                .withNotIndexed()
                .withNullableDefaultValue("default")
                .withNullableUILinkKey("linky")
                .withNullableUIName("myname")
                .withTransform(Transform.values())
                .withUIHidden()
                .build();
        assertThat("incorrect path", ir.getPath(), is(Optional.absent()));
        assertThat("incorrect default", ir.getDefaultValue(), is(Optional.of("default")));
        assertThat("incorrect key name", ir.getKeyName(), is("thunderpants"));
        assertThat("incorrect keyword type", ir.getKeywordType(), is(Optional.absent()));
        assertThat("incorrect source key", ir.getSourceKey(), is(Optional.of("whoop")));
        assertThat("incorrect transform", ir.getTransform(), is(Optional.of(Transform.values())));
        assertThat("incorrect link key", ir.getUiLinkKey(), is(Optional.of("linky")));
        assertThat("incorrect ui name", ir.getUiName(), is("myname"));
        assertThat("incorrect is derived", ir.isDerivedKey(), is(true));
        assertThat("incorrect is from parent", ir.isFromParent(), is(true));
        assertThat("incorrect is full text", ir.isFullText(), is(true));
        assertThat("incorrect is not indexed", ir.isNotIndexed(), is(true));
        assertThat("incorrect is ui hidden", ir.isUiHidden(), is(true));
    }
    
    @Test
    public void uinameNullsAndWhitespaceIgnored() {
        uiNameNullsAndWhitespaceIgnored(null);
        uiNameNullsAndWhitespaceIgnored("   \t   \n   ");
    }

    private void uiNameNullsAndWhitespaceIgnored(final String uiname) {
        final IndexingRules ir = IndexingRules.fromSourceKey("foo", "bar")
                .withNullableUIName(uiname)
                .build();
        assertThat("incorrect ui name", ir.getUiName(), is("Bar"));
    }
    
    @Test
    public void uiLinnkKeyNullsAndWhitespaceIgnored() {
        uiLinkKeyNullsAndWhitespaceIgnored(null);
        uiLinkKeyNullsAndWhitespaceIgnored("   \t   \n   ");
    }
    
    private void uiLinkKeyNullsAndWhitespaceIgnored(final String uilinkkey) {
        final IndexingRules ir = IndexingRules.fromSourceKey("foo", "bar")
                .withNullableUILinkKey(uilinkkey)
                .build();
        assertThat("incorrect ui name", ir.getUiName(), is("Bar"));
    }
    
    @Test
    public void fromPathFailNull() {
        try {
            IndexingRules.fromPath(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("path"));
        }
    }
    @Test
    public void fromSourceKeyFailNullWhitespace() {
        failFromSourceKey(null, "k",
                new IllegalArgumentException("sourceKey cannot be null or whitespace"));
        failFromSourceKey("   \t  \n ", "k",
                new IllegalArgumentException("sourceKey cannot be null or whitespace"));
        failFromSourceKey("s", null,
                new IllegalArgumentException("keyName cannot be null or whitespace"));
        failFromSourceKey("s", "   \t  \n ",
                new IllegalArgumentException("keyName cannot be null or whitespace"));
    }
    
    private void failFromSourceKey(
            final String sourceKey,
            final String keyName,
            final Exception expected) {
        try {
            IndexingRules.fromSourceKey(sourceKey, keyName);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void buildKeyNameFailNullWhitespace() {
        failBuildKeyName(null,
                new IllegalArgumentException("keyName cannot be null or whitespace"));
        failBuildKeyName("   \t \n  ",
                new IllegalArgumentException("keyName cannot be null or whitespace"));
    }
    
    private void failBuildKeyName(final String keyname, final Exception expected) {
        try {
            IndexingRules.fromSourceKey("s", "k").withKeyName(keyname);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void buildKeywordTypeFailNullWhitespace() {
        failBuildKeywordType(null,
                new IllegalArgumentException("keywordType cannot be null or whitespace"));
        failBuildKeywordType("   \t \n  ",
                new IllegalArgumentException("keywordType cannot be null or whitespace"));
    }
    
    private void failBuildKeywordType(final String keywordType, final Exception expected) {
        try {
            IndexingRules.fromSourceKey("s", "k").withKeywordType(keywordType);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
    
    @Test
    public void buildTransformWithSubobjectIDKeyAndSourceKey() {
        // check case where sub object id key is allowed in transform
        final IndexingRules ir = IndexingRules.fromSourceKey("k", "n")
                .withTransform(Transform.guid(new SearchObjectType("type", 1), "idkey"))
                .build();
        assertThat("incorrect source key", ir.getSourceKey(), is(Optional.of("k")));
        assertThat("incorrect key name", ir.getKeyName(), is("n"));
        assertThat("incorrect transform", ir.getTransform(),
                is(Optional.of(Transform.guid(new SearchObjectType("type", 1), "idkey"))));
    }
    
    @Test
    public void buildTransformFailNull() {
        failBuildTransform(IndexingRules.fromSourceKey("s", "k"), null,
                new NullPointerException("transform"));
    }
    
    @Test
    public void buildTransformFailSubObjectIDConstraint() throws Exception {
        failBuildTransform(IndexingRules.fromPath(new ObjectJsonPath("foo")),
                Transform.guid(new SearchObjectType("type", 1), "idkey"),
                new IllegalArgumentException("A transform with a subobject ID key is not " +
                        "compatible with a path. Path is: /foo"));
    }
    
    private void failBuildTransform(
            final Builder b,
            final Transform transform,
            final Exception expected) {
        try {
            b.withTransform(transform);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
        
    }
    
}
