package kbasesearchengine.test.tools;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import kbasesearchengine.test.common.TestCommon;
import kbasesearchengine.tools.SearchTools;

public class SearchToolsTest {
    
    //TODO TEST add many more tests. SearchTools is not remotely fully covered.

    private static Path tmpDir;
    
    @BeforeClass
    public static void setUp() throws Exception {
        tmpDir = Paths.get(TestCommon.getTempDir()).resolve(
                "SearchToolsTest-" + UUID.randomUUID());
        System.out.println("using temp dir " + tmpDir);
        Files.createDirectories(tmpDir);
    }
    
    @AfterClass
    public static void tearDown() throws Exception {
        if (TestCommon.getDeleteTempFiles()) {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }
    }
    
    private final String loadTestFile(final String file) {
        final InputStream is = getClass().getResourceAsStream(file);
        final Scanner s = new Scanner(is);
        s.useDelimiter("\\A");
        final String res = s.hasNext() ? s.next() : "";
        s.close();
        return res;
    }
    
    @Test
    public void createSpec() throws Exception {
        final Path yaml = Files.createTempFile(tmpDir, "SearchToolsTest", ".yaml");
        new SearchTools.MinimalSpecGenerator().generateMinimalSearchSpec(
                yaml, "WS", "Genome", "KBaseGenomes.Genome");
        
        final String expected = loadTestFile("createSpec.yaml");
        final String got = new String(Files.readAllBytes(yaml));
        
        assertThat("incorrect spec file", got, is(expected));
    }
    
    @Test
    public void createSpecFailSaveIO() throws Exception {
        failGenerateSpec(tmpDir, "Ws", "ST", "SOT", new IOException(tmpDir + ": Is a directory"));
        failGenerateSpec(Paths.get(""), "Ws", "ST", "SOT", new IOException(": Is a directory"));
    }
    
    @Test
    public void createSpecFailMissingArgs() throws Exception {
        failGenerateSpec(null, "WS", "ST", "SOT",
                new IllegalArgumentException("spec path is missing"));
        failGenerateSpec(Paths.get("foo"), null, "ST", "SOT",
                new IllegalArgumentException("storage type must be provided in arguments"));
        failGenerateSpec(Paths.get("foo"), "   \t   \n", "ST", "SOT",
                new IllegalArgumentException("storage type must be provided in arguments"));
        failGenerateSpec(Paths.get("foo"), "WS", null, "SOT",
                new IllegalArgumentException("search type must be provided in arguments"));
        failGenerateSpec(Paths.get("foo"), "WS", "   \t   \n", "SOT",
                new IllegalArgumentException("search type must be provided in arguments"));
        failGenerateSpec(Paths.get("foo"), "WS", "ST", null,
                new IllegalArgumentException("storage object type must be provided in arguments"));
        failGenerateSpec(Paths.get("foo"), "WS", "ST", "   \t   \n",
                new IllegalArgumentException("storage object type must be provided in arguments"));
    }
    
    private void failGenerateSpec(
            final Path specPath,
            final String storageType,
            final String searchType,
            final String storageObjectType,
            final Exception expected) {
        try {
            new SearchTools.MinimalSpecGenerator().generateMinimalSearchSpec(
                    specPath, storageType, searchType, storageObjectType);
            fail("expected exception");
        } catch (Exception got) {
            TestCommon.assertExceptionCorrect(got, expected);
        }
    }
}
