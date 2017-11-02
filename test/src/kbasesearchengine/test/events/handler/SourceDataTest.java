package kbasesearchengine.test.events.handler;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import kbasesearchengine.events.handler.SourceData;
import kbasesearchengine.test.common.TestCommon;
import us.kbase.common.service.UObject;

public class SourceDataTest {
    
    @Test
    public void buildMimimal() {
        final SourceData sd = SourceData.getBuilder(
                new UObject(ImmutableMap.of("foo", "bar")), "name", "creator").build();
        
        assertThat("incorrect data", sd.getData().asClassInstance(Map.class),
                is(ImmutableMap.of("foo", "bar")));
        assertThat("incorrect name", sd.getName(), is("name"));
        assertThat("incorrect creator", sd.getCreator(), is("creator"));
        assertThat("incorrect copier", sd.getCopier(), is(Optional.absent()));
        assertThat("incorrect commit", sd.getCommitHash(), is(Optional.absent()));
        assertThat("incorrect method", sd.getMethod(), is(Optional.absent()));
        assertThat("incorrect module", sd.getModule(), is(Optional.absent()));
        assertThat("incorrect version", sd.getVersion(), is(Optional.absent()));
        assertThat("incorrect md5", sd.getMD5(), is(Optional.absent()));
    }
    
    @Test
    public void buildMaximal() {
        final SourceData sd = SourceData.getBuilder(
                new UObject(ImmutableMap.of("foo1", "bar1")), "name1", "creator1")
                .withNullableCommitHash("commit")
                .withNullableCopier("copier")
                .withNullableMethod("meth")
                .withNullableModule("mod")
                .withNullableVersion("ver")
                .withNullableMD5("md5")
                .build();
        
        assertThat("incorrect data", sd.getData().asClassInstance(Map.class),
                is(ImmutableMap.of("foo1", "bar1")));
        assertThat("incorrect name", sd.getName(), is("name1"));
        assertThat("incorrect creator", sd.getCreator(), is("creator1"));
        assertThat("incorrect copier", sd.getCopier(), is(Optional.of("copier")));
        assertThat("incorrect commit", sd.getCommitHash(), is(Optional.of("commit")));
        assertThat("incorrect method", sd.getMethod(), is(Optional.of("meth")));
        assertThat("incorrect module", sd.getModule(), is(Optional.of("mod")));
        assertThat("incorrect version", sd.getVersion(), is(Optional.of("ver")));
        assertThat("incorrect md5", sd.getMD5(), is(Optional.of("md5")));
    }
    
    @Test
    public void buildNull() {
        final SourceData sd = SourceData.getBuilder(
                new UObject(ImmutableMap.of("foo2", "bar2")), "name2", "creator2")
                .withNullableCommitHash(null)
                .withNullableCopier(null)
                .withNullableMethod(null)
                .withNullableModule(null)
                .withNullableVersion(null)
                .withNullableMD5(null)
                .build();
        
        assertThat("incorrect data", sd.getData().asClassInstance(Map.class),
                is(ImmutableMap.of("foo2", "bar2")));
        assertThat("incorrect name", sd.getName(), is("name2"));
        assertThat("incorrect creator", sd.getCreator(), is("creator2"));
        assertThat("incorrect copier", sd.getCopier(), is(Optional.absent()));
        assertThat("incorrect commit", sd.getCommitHash(), is(Optional.absent()));
        assertThat("incorrect method", sd.getMethod(), is(Optional.absent()));
        assertThat("incorrect module", sd.getModule(), is(Optional.absent()));
        assertThat("incorrect version", sd.getVersion(), is(Optional.absent()));
        assertThat("incorrect md5", sd.getMD5(), is(Optional.absent()));
    }
    
    @Test
    public void buildEmpty() {
        final SourceData sd = SourceData.getBuilder(
                new UObject(ImmutableMap.of("foo2", "bar2")), "name2", "creator2")
                .withNullableCommitHash("   \t   \n   ")
                .withNullableCopier("   \t   \n   ")
                .withNullableMethod("   \t   \n   ")
                .withNullableModule("   \t   \n   ")
                .withNullableVersion("   \t   \n   ")
                .withNullableMD5("   \t   \n   ")
                .build();
        
        assertThat("incorrect data", sd.getData().asClassInstance(Map.class),
                is(ImmutableMap.of("foo2", "bar2")));
        assertThat("incorrect name", sd.getName(), is("name2"));
        assertThat("incorrect creator", sd.getCreator(), is("creator2"));
        assertThat("incorrect copier", sd.getCopier(), is(Optional.absent()));
        assertThat("incorrect commit", sd.getCommitHash(), is(Optional.absent()));
        assertThat("incorrect method", sd.getMethod(), is(Optional.absent()));
        assertThat("incorrect module", sd.getModule(), is(Optional.absent()));
        assertThat("incorrect version", sd.getVersion(), is(Optional.absent()));
        assertThat("incorrect md5", sd.getMD5(), is(Optional.absent()));
    }
    
    @Test
    public void getBuilderFail() {
        final UObject d = new UObject(new HashMap<>());
        
        failGetBuilder(null, "n", "c", new NullPointerException("data"));
        failGetBuilder(d, null, "c",
                new IllegalArgumentException("name cannot be null or the empty string"));
        failGetBuilder(d, "   \n \t", "c",
                new IllegalArgumentException("name cannot be null or the empty string"));
        failGetBuilder(d, "n", null,
                new IllegalArgumentException("creator cannot be null or the empty string"));
        failGetBuilder(d, "n", "  \n \t   \n ",
                new IllegalArgumentException("creator cannot be null or the empty string"));
    }

    private void failGetBuilder(
            final UObject data,
            final String name,
            final String creator,
            final Exception expected) {
        try {
            SourceData.getBuilder(data, name, creator);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
