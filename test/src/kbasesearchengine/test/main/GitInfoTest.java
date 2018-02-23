package kbasesearchengine.test.main;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.file.Paths;

import org.junit.Test;

import kbasesearchengine.main.GitInfo;

public class GitInfoTest {
    
    @Test
    public void goodFile() {
        final GitInfo gi = new GitInfo(Paths.get(
                "/kbasesearchengine/test/main/data/gitgood.properties"));
        
        assertThat("incorrect url", gi.getGitUrl(), is("some url"));
        assertThat("incorrect commit", gi.getGitCommit(), is("some commit"));
    }
    
    @Test
    public void badFile() {
        final GitInfo gi = new GitInfo(Paths.get(
                "/kbasesearchengine/test/main/data/gitgood2.properties"));
        
        final String error = "Unable to load git properties file from " +
                    "/kbasesearchengine/test/main/data/gitgood2.properties";
        
        assertThat("incorrect url", gi.getGitUrl(), is(error));
        assertThat("incorrect commit", gi.getGitCommit(), is(error));
    }
    
    @Test
    public void missingUrl() {
        final GitInfo gi = new GitInfo(Paths.get(
                "/kbasesearchengine/test/main/data/gitmissingurl.properties"));
        
        final String error = "Missing key from git properties file " +
                "/kbasesearchengine/test/main/data/gitmissingurl.properties";
        
        assertThat("incorrect url", gi.getGitUrl(), is(error));
        assertThat("incorrect commit", gi.getGitCommit(), is(error));
    }
    
    @Test
    public void missingCommit() {
        final GitInfo gi = new GitInfo(Paths.get(
                "/kbasesearchengine/test/main/data/gitmissingcommit.properties"));
        
        final String error = "Missing key from git properties file " +
                "/kbasesearchengine/test/main/data/gitmissingcommit.properties";
        
        assertThat("incorrect url", gi.getGitUrl(), is(error));
        assertThat("incorrect commit", gi.getGitCommit(), is(error));
    }

}
