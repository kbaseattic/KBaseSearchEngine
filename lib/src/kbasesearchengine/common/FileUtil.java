package kbasesearchengine.common;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by apasha on 12/12/17.
 */
public class FileUtil {

    /**
     * Returns a sub directory with path rootDir/subDirName. Creates this
     * sub-directory if it does not already exist under rootDir.
     *
     * @param rootDir a valid file
     * @param subDirName sub directory name
     * @return the sub directory rootDir/subName
     */
    public static File getOrCreateSubDir(final File rootDir, final String subDirName) {

        File ret = new File(rootDir, subDirName);
        if (!ret.exists()) {
            ret.mkdirs();
        }

        return ret;
    }

    /**
     * Returns an empty sub directory with path rootDir/subDirName if
     * it does not already exist under rootDir.
     *
     * If the sub directory already exists under rootDir and contains data,
     * this data is deleted.
     *
     * @param rootDir a valid directory
     * @param subDirName sub directory name
     * @return the sub directory rootTempDir/subName
     */
    public static File getOrCreateCleanSubDir(final File rootDir, String subDirName)
                                                              throws IOException {

        File subDir = getOrCreateSubDir(rootDir, subDirName);

        if ( subDir.list().length > 0 ) {      // clean sub dir if not empty

            String path = subDir.getAbsolutePath();
            FileUtils.deleteDirectory(subDir);
            subDir = new File(path);
            subDir.mkdirs();
        }

        return subDir;
    }
}
