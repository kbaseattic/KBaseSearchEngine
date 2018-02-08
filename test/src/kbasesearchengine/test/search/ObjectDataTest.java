package kbasesearchengine.test.search;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.common.GUID;
import kbasesearchengine.search.ObjectData;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.test.common.TestCommon;
import nl.jqno.equalsverifier.EqualsVerifier;

public class ObjectDataTest {

    @Test
    public void equals() {
        EqualsVerifier.forClass(ObjectData.class).usingGetClass().verify();
    }
    
    @Test
    public void buildMinimal() {
        final ObjectData od = ObjectData.getBuilder(new GUID("Foo:1/2/3")).build();
        
        assertThat("incorrect guid", od.getGUID(), is(new GUID("Foo:1/2/3")));
        assertThat("incorrect parent guid", od.getParentGUID(), is(Optional.absent()));
        assertThat("incorrect hash", od.getCommitHash(), is(Optional.absent()));
        assertThat("incorrect copier", od.getCopier(), is(Optional.absent()));
        assertThat("incorrect creator", od.getCreator(), is(Optional.absent()));
        assertThat("incorrect data", od.getData(), is(Optional.absent()));
        assertThat("incorrect key props", od.getKeyProperties(), is(Collections.emptyMap()));
        assertThat("incorrect md5", od.getMd5(), is(Optional.absent()));
        assertThat("incorrect method", od.getMethod(), is(Optional.absent()));
        assertThat("incorrect module", od.getModule(), is(Optional.absent()));
        assertThat("incorrect mod ver", od.getModuleVersion(), is(Optional.absent()));
        assertThat("incorrect obj name", od.getObjectName(), is(Optional.absent()));
        assertThat("incorrect parent data", od.getParentData(), is(Optional.absent()));
        assertThat("incorrect timestamp", od.getTimestamp(), is(Optional.absent()));
        assertThat("incorrect type", od.getType(), is(Optional.absent()));
        assertThat("incorrect highlight", od.getKeyProperties(), is(Collections.emptyMap()));
    }
    
    @Test
    public void buildNulls() {
        final ObjectData od = ObjectData.getBuilder(new GUID("Foo:1/2/3"))
                .withNullableCommitHash(null)
                .withNullableCopier(null)
                .withNullableCreator(null)
                .withNullableData(null)
                .withNullableMD5(null)
                .withNullableMethod(null)
                .withNullableModule(null)
                .withNullableModuleVersion(null)
                .withNullableObjectName(null)
                .withNullableParentData(null)
                .withNullableTimestamp(null)
                .withNullableType(null)
                .build();
        
        assertThat("incorrect guid", od.getGUID(), is(new GUID("Foo:1/2/3")));
        assertThat("incorrect parent guid", od.getParentGUID(), is(Optional.absent()));
        assertThat("incorrect hash", od.getCommitHash(), is(Optional.absent()));
        assertThat("incorrect copier", od.getCopier(), is(Optional.absent()));
        assertThat("incorrect creator", od.getCreator(), is(Optional.absent()));
        assertThat("incorrect data", od.getData(), is(Optional.absent()));
        assertThat("incorrect key props", od.getKeyProperties(), is(Collections.emptyMap()));
        assertThat("incorrect md5", od.getMd5(), is(Optional.absent()));
        assertThat("incorrect method", od.getMethod(), is(Optional.absent()));
        assertThat("incorrect module", od.getModule(), is(Optional.absent()));
        assertThat("incorrect mod ver", od.getModuleVersion(), is(Optional.absent()));
        assertThat("incorrect obj name", od.getObjectName(), is(Optional.absent()));
        assertThat("incorrect parent data", od.getParentData(), is(Optional.absent()));
        assertThat("incorrect timestamp", od.getTimestamp(), is(Optional.absent()));
        assertThat("incorrect type", od.getType(), is(Optional.absent()));
        assertThat("incorrect highlight", od.getKeyProperties(), is(Collections.emptyMap()));

    }
    
    @Test
    public void buildWhitespace() {
        final ObjectData od = ObjectData.getBuilder(new GUID("Foo:1/2/3"))
                .withNullableCommitHash("   \t  \n  ")
                .withNullableCopier("   \t  \n  ")
                .withNullableCreator("   \t  \n  ")
                .withNullableData(null)
                .withNullableMD5("   \t  \n  ")
                .withNullableMethod("   \t  \n  ")
                .withNullableModule("   \t  \n  ")
                .withNullableModuleVersion("   \t  \n  ")
                .withNullableObjectName("   \t  \n  ")
                .withNullableParentData(null)
                .withNullableTimestamp(null)
                .withNullableType(null)
                .build();
        
        assertThat("incorrect guid", od.getGUID(), is(new GUID("Foo:1/2/3")));
        assertThat("incorrect parent guid", od.getParentGUID(), is(Optional.absent()));
        assertThat("incorrect hash", od.getCommitHash(), is(Optional.absent()));
        assertThat("incorrect copier", od.getCopier(), is(Optional.absent()));
        assertThat("incorrect creator", od.getCreator(), is(Optional.absent()));
        assertThat("incorrect data", od.getData(), is(Optional.absent()));
        assertThat("incorrect key props", od.getKeyProperties(), is(Collections.emptyMap()));
        assertThat("incorrect md5", od.getMd5(), is(Optional.absent()));
        assertThat("incorrect method", od.getMethod(), is(Optional.absent()));
        assertThat("incorrect module", od.getModule(), is(Optional.absent()));
        assertThat("incorrect mod ver", od.getModuleVersion(), is(Optional.absent()));
        assertThat("incorrect obj name", od.getObjectName(), is(Optional.absent()));
        assertThat("incorrect parent data", od.getParentData(), is(Optional.absent()));
        assertThat("incorrect timestamp", od.getTimestamp(), is(Optional.absent()));
        assertThat("incorrect type", od.getType(), is(Optional.absent()));
        assertThat("incorrect highlight", od.getKeyProperties(), is(Collections.emptyMap()));

    }
    
    @Test
    public void buildMaximal() throws Exception {
        final ObjectData od = ObjectData.getBuilder(new GUID("Foo:1/2/3:sub/thing"))
                .withNullableCommitHash("hash")
                .withNullableCopier("copy")
                .withNullableCreator("create")
                .withNullableData(ImmutableMap.of("foo", "bar"))
                .withNullableMD5("md5")
                .withNullableMethod("meth")
                .withNullableModule("mod")
                .withNullableModuleVersion("ver")
                .withNullableObjectName("oname")
                .withNullableParentData(ImmutableMap.of("whee", "whoo"))
                .withNullableTimestamp(Instant.ofEpochMilli(10000))
                .withNullableType(new SearchObjectType("foo", 1))
                .withKeyProperty("baz", "bat")
                .withKeyProperty("null", null)
                .withKeyProperty("ws", "   \t   \n   ")
                .withHighlight("field", new ArrayList<>(Arrays.asList("match")))
                .build();
        
        assertThat("incorrect guid", od.getGUID(), is(new GUID("Foo:1/2/3:sub/thing")));
        assertThat("incorrect parent guid", od.getParentGUID(),
                is(Optional.of(new GUID("Foo:1/2/3"))));
        assertThat("incorrect hash", od.getCommitHash(), is(Optional.of("hash")));
        assertThat("incorrect copier", od.getCopier(), is(Optional.of("copy")));
        assertThat("incorrect creator", od.getCreator(), is(Optional.of("create")));
        assertThat("incorrect data", od.getData(),
                is(Optional.of(ImmutableMap.of("foo", "bar"))));
        
        final Map<String, Object> keyprops = new HashMap<>();
        keyprops.put("baz", "bat");
        keyprops.put("null", null);
        keyprops.put("ws", "   \t   \n   ");
        assertThat("incorrect key props", od.getKeyProperties(), is(keyprops));

        final Map<String, List<String>> highlight = new HashMap<>();
        highlight.put("field", Arrays.asList("match"));
        assertThat("incorrect highlight", od.getHighlight(), is(highlight));
        assertThat("incorrect md5", od.getMd5(), is(Optional.of("md5")));
        assertThat("incorrect method", od.getMethod(), is(Optional.of("meth")));
        assertThat("incorrect module", od.getModule(), is(Optional.of("mod")));
        assertThat("incorrect mod ver", od.getModuleVersion(), is(Optional.of("ver")));
        assertThat("incorrect obj name", od.getObjectName(), is(Optional.of("oname")));
        assertThat("incorrect parent data", od.getParentData(),
                is(Optional.of(ImmutableMap.of("whee", "whoo"))));
        assertThat("incorrect timestamp", od.getTimestamp(),
                is(Optional.of(Instant.ofEpochMilli(10000))));
        assertThat("incorrect type", od.getType(),
                is(Optional.of(new SearchObjectType("foo", 1))));
    }
    
    @Test
    public void buildFail() {
        try {
            ObjectData.getBuilder(null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, new NullPointerException("guid"));
        }
    }
    
    @Test
    public void withKeyFail() {
        final ObjectData.Builder b = ObjectData.getBuilder(new GUID("a:1/2/3"));
        failWithKey(b, null, new IllegalArgumentException(
                "key cannot be null or whitespace"));
        failWithKey(b, "    \t    \n  ", new IllegalArgumentException(
                "key cannot be null or whitespace"));
    }
    
    private void failWithKey(
            final ObjectData.Builder b,
            final String key,
            final Exception expected) {
        try {
            b.withKeyProperty(key, null);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }

    @Test
    public void withHighlightFail() {
        final ObjectData.Builder b = ObjectData.getBuilder(new GUID("ws:1/2/3"));
        failWithHighlight(b, null, new ArrayList<>(Arrays.asList("highlight")),
                new IllegalArgumentException("field cannot be null or whitespace"));
        failWithHighlight(b, "   ", new ArrayList<>(Arrays.asList("highlight")),
                new IllegalArgumentException("field cannot be null or whitespace"));
        failWithHighlight(b, "field", null,
                new NullPointerException("highlight list cannot be null"));
        failWithHighlight(b, "field", new ArrayList<>(Arrays.asList("  ")),
                new IllegalArgumentException("highlight cannot be null or whitespace"));

        ArrayList<String> nullStrings = new ArrayList<>();
        nullStrings.add(null);
        failWithHighlight(b, "field", nullStrings,
                new IllegalArgumentException("highlight cannot be null or whitespace"));

        //test immutability of highlight map
        b.withHighlight("field", new ArrayList<>(Arrays.asList("highlight")));
        ObjectData obj = b.build();
        Map<String, List<String>> res = obj.getHighlight();

        try {
            res.put("test", new ArrayList<>(Arrays.asList("highlight")));
            fail("cannot modify highlight");
        } catch(Exception e){
            TestCommon.assertExceptionCorrect(e, new UnsupportedOperationException());
        }

        try {
            List<String> list = res.get("field");
            list.add("test");
            fail("cannot modify highlight list");
        } catch(Exception e){
            TestCommon.assertExceptionCorrect(e, new UnsupportedOperationException());
        }


    }

    public void failWithHighlight(
           final ObjectData.Builder b,
           final String field,
           final List<String> highlight,
           final Exception expected ) {

        try {
            b.withHighlight(field, highlight);
            fail("expected exception for incorrect highlight");
        } catch (Exception e) {
            TestCommon.assertExceptionCorrect(e, expected);
        }
    }

}
