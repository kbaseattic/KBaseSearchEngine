package kbaserelationengine.test.controllers.workspace;

import static us.kbase.common.test.controllers.ControllerCommon.findFreePort;
import static us.kbase.common.test.controllers.ControllerCommon.makeTempDirs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import us.kbase.common.test.TestException;


/** Q&D Utility to run a Workspace server for the purposes of testing from
 * Java. Ideally this'll be swapped out for a Docker container that runs automatically with
 * kb-sdk test at some point.
 * @author gaprice@lbl.gov
 *
 */
public class WorkspaceController {
    
    private final static String DATA_DIR = "data";
    private static final String WS_CLASS = "us.kbase.workspace.WorkspaceServer";
    
    private final static List<String> tempDirectories = new LinkedList<String>();
    static {
        tempDirectories.add(DATA_DIR);
    }
    
    private final Path tempDir;
    
    private final Process workspace;
    private final int port;

    public WorkspaceController(
            final Path jarsDir,
            final String workspaceVersion,
            final URL authServiceRootURL,
            final Path rootTempDir)
            throws Exception {
        final String classpath = loadVersion(jarsDir, workspaceVersion);
        tempDir = makeTempDirs(rootTempDir, "WorkspaceController-", tempDirectories);
        port = findFreePort();

        //TODO NOW set up deploy.cfg & KB_DEP...
        
        final List<String> command = new LinkedList<String>();
        command.addAll(Arrays.asList("java", "-classpath", classpath, WS_CLASS, "" + port));
        System.out.println(command);
        final ProcessBuilder servpb = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(tempDir.resolve("workspace.log").toFile());
        
        workspace = servpb.start();
        Thread.sleep(2000); //wait for server to start up
    }

    private String loadVersion(final Path jarsDir, final String workspaceVersion)
            throws IOException {
        final InputStream is = getClass().getResourceAsStream(
                "version_" + workspaceVersion);
        if (is == null) {
            throw new TestException("No workspace versions file version_" + workspaceVersion);
        }
        //TODO NOW check workspace jar exists
        final List<String> classpath = new LinkedList<>();
        try (final Reader r = new InputStreamReader(is)) {
            final BufferedReader br = new BufferedReader(r);
            String line;
            while ((line = br.readLine()) != null) {
                classpath.add(jarsDir.resolve("lib/jars").resolve(line).toString());
            }
        }
        return String.join(":", classpath);
    }

    public int getServerPort() {
        return port;
    }
    
    public Path getTempDir() {
        return tempDir;
    }
    
    public void destroy(boolean deleteTempFiles) throws IOException {
        if (workspace != null) {
            workspace.destroy();
        }
        if (tempDir != null && deleteTempFiles) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    public static void main(String[] args) throws Exception {
        WorkspaceController ac = new WorkspaceController(
                Paths.get("/home/crusherofheads/localgit/jars"),
                "0.7.2-dev1",
                new URL("https://ci.kbase.us/services/auth"),
                Paths.get("workspacetesttemp"));
        System.out.println(ac.getServerPort());
        System.out.println(ac.getTempDir());
        Scanner reader = new Scanner(System.in);
        System.out.println("any char to shut down");
        //get user input for a
        reader.next();
        ac.destroy(false);
        reader.close();
    }
    
}

