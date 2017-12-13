package kbasesearchengine.test.common;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


import kbasesearchengine.common.FileUtil;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Created by apasha on 12/13/17.
 */
public class FileUtilTest {
    private static Path tempDirPath;
    private static final String subDirName = "testSubDir";


    @BeforeClass
    public static void setup() throws Exception {
        tempDirPath = Paths.get(TestCommon.getTempDir()).resolve("FileUtilTest");
        tempDirPath.toFile().mkdirs();
    }

    @AfterClass
    public static void teardown() throws Exception {
        FileUtils.deleteDirectory(tempDirPath.toFile());
    }

    /* dir does not exist and needs to be created */
    @Test
    public void getOrCreateSubDirTest1() {
        File subDir = FileUtil.getOrCreateSubDir(tempDirPath.toFile(), subDirName);
        assertTrue("subdir was not created", subDir.exists());
        assertTrue("subdir not a directory", subDir.isDirectory());
    }

    /* dir already exists */
    @Test
    public void getOrCreateSubDirTest2() {
        File subDir;
        subDir = new File(tempDirPath.toFile(), subDirName);
        subDir.mkdirs();
        assertTrue("subdir was not created", subDir.exists());
        assertTrue("subdir not a directory", subDir.isDirectory());

        getOrCreateSubDirTest1();
    }

    @Test
    public void getOrCreateCleanSubDirTest() {

        File subDir = FileUtil.getOrCreateSubDir(tempDirPath.toFile(), subDirName);
        assertTrue("subdir was not created", subDir.exists());
        assertTrue("subdir not a directory", subDir.isDirectory());

        // add files to subDir
        try {
            File.createTempFile("test_", ".tmp", subDir);
            assertTrue("dir is empty", subDir.list().length == 1);

            subDir = FileUtil.getOrCreateCleanSubDir(tempDirPath.toFile(), subDirName);
            assertTrue("dir is not empty", subDir.list().length == 0);
        } catch (IOException ioe) {
            fail();
        }

    }
}
