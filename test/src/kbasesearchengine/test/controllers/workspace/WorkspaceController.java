package kbasesearchengine.test.controllers.workspace;

import static us.kbase.common.test.controllers.ControllerCommon.findFreePort;
import static us.kbase.common.test.controllers.ControllerCommon.makeTempDirs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import us.kbase.common.test.TestException;


/** Q&D Utility to run a Workspace server for the purposes of testing from
 * Java. Ideally this'll be swapped out for a Docker container that runs automatically with
 * kb-sdk test at some point.
 * 
 * Initializes a GridFS backend and does not support handles.
 * @author gaprice@lbl.gov
 *
 */
public class WorkspaceController {
    
    private final static String DATA_DIR = "temp_data";
    private static final String WS_CLASS = "us.kbase.workspace.WorkspaceServer";
    
    private final static List<String> tempDirectories = new LinkedList<String>();
    static {
        tempDirectories.add(DATA_DIR);
    }
    
    private final Path tempDir;
    
    private final Process workspace;
    private final int port;

    public WorkspaceController(
            final String workspaceVersion,
            final Path jarsDir,
            final String mongoHost,
            final String mongoDatabase,
            final String searchMongoDB,
            final String adminUser,
            final URL authServiceRootURL,
            final Path rootTempDir)
            throws Exception {
        final String classpath = loadVersion(jarsDir, workspaceVersion);
        tempDir = makeTempDirs(rootTempDir, "WorkspaceController-", tempDirectories);
        port = findFreePort();
        final Path deployCfg = createDeployCfg(
                mongoHost, mongoDatabase, searchMongoDB, authServiceRootURL, adminUser);
        
        try (final MongoClient mc = new MongoClient(mongoHost)) {
            final MongoDatabase db = mc.getDatabase(mongoDatabase);
            db.getCollection("settings").insertOne(
                    new Document("type_db", "WorkspaceController_types")
                        .append("backend", "gridFS"));
        }
        
        final List<String> command = new LinkedList<String>();
        command.addAll(Arrays.asList("java", "-classpath", classpath, WS_CLASS, "" + port));
        final ProcessBuilder servpb = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(tempDir.resolve("workspace.log").toFile());
        
        final Map<String, String> env = servpb.environment();
        env.put("KB_DEPLOYMENT_CONFIG", deployCfg.toString());
        
        workspace = servpb.start();
        Thread.sleep(5000); //wait for server to start up
    }

    private Path createDeployCfg(
            final String mongoHost,
            final String mongoDatabase,
            final String searchMongoDB,
            final URL authRootURL,
            final String adminUser)
            throws IOException {
        final File iniFile = new File(tempDir.resolve("test.cfg").toString());
        System.out.println("Created temporary workspace config file: " +
                iniFile.getAbsolutePath());
        final Ini ini = new Ini();
        Section ws = ini.add("Workspace");
        ws.add("mongodb-host", mongoHost);
        ws.add("mongodb-database", mongoDatabase);
        ws.add("auth-service-url", authRootURL.toString() + "/api/legacy/KBase");
        ws.add("auth-service-url-allow-insecure", "true");
        ws.add("globus-url", authRootURL.toString() + "/api/legacy/Globus");
        ws.add("ws-admin", adminUser);
        ws.add("temp-dir", tempDir.resolve("temp_data"));
        ws.add("ignore-handle-service", "true");
        // search listener config
        ws.add("listeners", "search");
        ws.add("listener-search-class",
                "us.kbase.workspace.modules.SearchPrototypeEventHandlerFactory");
        ws.add("listener-search-config-mongohost", mongoHost);
        ws.add("listener-search-config-mongodatabase", searchMongoDB);
        ini.store(iniFile);
        return iniFile.toPath();
    }
    
    private String loadVersion(final Path jarsDir, final String workspaceVersion)
            throws IOException {
        final InputStream is = getClass().getResourceAsStream("version_" + workspaceVersion);
        if (is == null) {
            throw new TestException("No workspace versions file version_" + workspaceVersion);
        }
        final List<String> classpath = new LinkedList<>();
        try (final Reader r = new InputStreamReader(is)) {
            final BufferedReader br = new BufferedReader(r);
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    final Path jarPath = jarsDir.resolve(line);
                    if (Files.notExists(jarPath)) {
                        throw new TestException("Required jar does not exist: " + jarPath);
                        
                    }
                    classpath.add(jarPath.toString());
                }
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
                "0.7.2-dev1",
                Paths.get("/home/crusherofheads/localgit/jars"),
                "localhost:27017",
                "WSController",
                "SearchDB",
                "workspaceadmin",
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

