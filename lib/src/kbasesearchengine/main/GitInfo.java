package kbasesearchengine.main;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/** Extracts information about the git repo the software is stored in from a properties file.
 * @author gaprice@lbl.gov
 *
 */
public class GitInfo {
    
    private static final Path DEFAULT_FILE_LOCATION = Paths.get("/git.properties");
    private static final String LOAD_ERROR = "Unable to load git properties file from ";
    private static final String KEY_ERROR = "Missing key from git properties file ";
    private static final String URL = "giturl";
    private static final String COMMIT = "commit";
    
    private final String gitUrl;
    private final String gitCommit;
    
    /** Get git information from the default file location. */
    public GitInfo() {
        this(DEFAULT_FILE_LOCATION);
    }

    /** Get git information from a specified file location. Mainly used for testing.
     * @param gitPropertiesFileLocation the file location.
     */
    public GitInfo(final Path gitPropertiesFileLocation) {
        final Properties gitprops = new Properties();
        final InputStream gitfile =
                GitInfo.class.getResourceAsStream(gitPropertiesFileLocation.toString());
        if (gitfile == null) {
            gitUrl = LOAD_ERROR + gitPropertiesFileLocation;
            gitCommit = LOAD_ERROR + gitPropertiesFileLocation;
            return;
        }
        try {
            gitprops.load(gitfile);
        } catch (IOException e) {
            // not sure how to test this. Untested for now.
            gitUrl = LOAD_ERROR + gitPropertiesFileLocation;
            gitCommit = LOAD_ERROR + gitPropertiesFileLocation;
            return;
        }
        if (gitprops.get(URL) == null || gitprops.get(COMMIT) == null) {
            gitUrl = KEY_ERROR + gitPropertiesFileLocation;
            gitCommit = KEY_ERROR + gitPropertiesFileLocation;
            return;
        }
        gitUrl = (String) gitprops.get(URL);
        gitCommit = (String) gitprops.get(COMMIT);
    }

    /** Get the git url.
     * @return the url.
     */
    public String getGitUrl() {
        return gitUrl;
    }

    /** Get the git commit.
     * @return the commit.
     */
    public String getGitCommit() {
        return gitCommit;
    }
}
