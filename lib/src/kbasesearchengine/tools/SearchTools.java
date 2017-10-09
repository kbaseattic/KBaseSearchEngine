package kbasesearchengine.tools;

import static kbasesearchengine.tools.Utils.nonNull;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Optional;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.tools.SearchToolsConfig.SearchToolsConfigException;
import kbasesearchengine.tools.WorkspaceEventGenerator.EventGeneratorException;

/** Tools for working with Search. Note that this CLI is designed against the search prototype
 * event listener in the workspace service, and may need changes if the event listener is changed
 * or removed.
 * @author gaprice@lbl.gov
 *
 */
public class SearchTools {
    
    private static final String NAME = "search_tools";

    /** Runs the CLI.
     * @param args the program arguments.
     */
    public static void main(String[] args) {
        System.exit(new SearchTools(args, System.out, System.err).execute());
    }
    
    private final String[] args;
    private final PrintStream out;
    private final PrintStream err;
    
    private MongoDatabase workspaceDB = null;
    private MongoDatabase searchDB = null;

    /** Create a new CLI instance.
     * @param args the program arguments.
     * @param console the system console.
     * @param out the out printstream.
     * @param err the error printstream.
     */
    public SearchTools(
            final String[] args,
            final PrintStream out,
            final PrintStream err) {
        nonNull(args, "args");
        nonNull(out, "out");
        nonNull(err, "err");
        this.args = args;
        this.out = out;
        this.err = err;
        quietLogger();
    }
    
    private void quietLogger() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .setLevel(Level.OFF);
    }
    
    /** Execute the CLI command.
     * @return the exit code.
     */
    public int execute() {
        final Args a = new Args();
        JCommander jc = new JCommander(a);
        jc.setProgramName(NAME);

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            printError(e, a.verbose);
            return 1;
        }
        if (a.help) {
            usage(jc);
            return 0;
        }
        final SearchToolsConfig cfg;
        try {
            cfg = getConfig(a.configPath);
        } catch (NoSuchFileException e) {
            printError("No such file", e, a.verbose);
            return 1;
        } catch (AccessDeniedException e) {
            printError("Access denied", e, a.verbose);
            return 1;
        } catch (IOException e) {
            printError(e, a.verbose);
            return 1;
        } catch (SearchToolsConfigException e) {
            printError("For config file " + a.configPath, e, a.verbose);
            return 1;
        }
        try {
            setUpMongoDBs(cfg, a.genWSEvents, a.dropDB);
        } catch (MongoException e) {
            printError(e, a.verbose);
            return 1;
        }
        boolean noCommand = true; // this seems dumb...
        if (a.dropDB) {
            try {
                deleteMongoDB();
                noCommand = false;
            } catch (MongoException e) {
                printError(e, a.verbose);
                return 1;
            }
        }
        if (a.genWSEvents) {
            try {
                runEventGenerator(out, a.ref, a.verbose,
                        getWsBlackList(a.wsBlacklist, cfg.getWorkspaceBlackList()),
                        getWsTypes(a.wsTypes, cfg.getWorkspaceTypes()));
                noCommand = false;
            } catch (EventGeneratorException e) {
                printError(e, a.verbose);
                return 1;
            }
        }
        if (noCommand) {
            usage(jc);
        }
        return 0;
        
    }
    
    private void setUpMongoDBs(
            final SearchToolsConfig cfg,
            final boolean genWSEvents,
            final boolean dropDB)
            throws MongoException {
        
        if (!genWSEvents && !dropDB) {
            return;
        }
        if (genWSEvents) {
            setUpBothMongoClients(cfg);
        } else {
            setUpSearchMongoClient(cfg);
        }
    }

    private void setUpSearchMongoClient(final SearchToolsConfig cfg) {
        // should close connection, but we rely on the program exiting to do that here
        final MongoClient searchClient = getClient(cfg.getSearchMongoHost(), cfg.getSearchMongoDB(),
                cfg.getSearchMongoUser(), cfg.getSearchMongoPwd());
        searchDB = searchClient.getDatabase(cfg.getSearchMongoDB());
    }

    private void setUpBothMongoClients(final SearchToolsConfig cfg) {
        // should close connection, but we rely on the program exiting to do that here
        final MongoClient searchClient;
        final MongoClient wsClient;
        if (cfg.getSearchMongoHost().equals(cfg.getWorkspaceMongoHost())) {
            final List<MongoCredential> creds = new LinkedList<>();
            addCred(cfg.getSearchMongoDB(), cfg.getSearchMongoUser(),
                    cfg.getSearchMongoPwd(), creds);
            addCred(cfg.getWorkspaceMongoDB(), cfg.getWorkspaceMongoUser(),
                    cfg.getWorkspaceMongoPwd(), creds);
            searchClient = new MongoClient(new ServerAddress(cfg.getSearchMongoHost()), creds);
            wsClient = searchClient;
        } else {
            searchClient = getClient(cfg.getSearchMongoHost(), cfg.getSearchMongoDB(),
                    cfg.getSearchMongoUser(), cfg.getSearchMongoPwd());
            wsClient = getClient(cfg.getWorkspaceMongoHost(), cfg.getWorkspaceMongoDB(),
                    cfg.getWorkspaceMongoUser(), cfg.getWorkspaceMongoPwd());
            
        }
        searchDB = searchClient.getDatabase(cfg.getSearchMongoDB());
        workspaceDB = wsClient.getDatabase(cfg.getWorkspaceMongoDB());
    }

    private MongoClient getClient(
            final String host,
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd) {
        return new MongoClient(new ServerAddress(host), getCred(db, user, pwd));
    }

    private List<MongoCredential> getCred(
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd) {
        final List<MongoCredential> creds = new LinkedList<>();
        addCred(db, user, pwd, creds);
        return creds;
    }

    private void addCred(
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd,
            final List<MongoCredential> creds) {
        if (user.isPresent()) {
            creds.add(MongoCredential.createCredential(user.get(), db, pwd.get()));
        }
    }
    
    private List<String> getWsTypes(
            List<String> args,
            final List<String> config) {
        if (args == null) {
            args = new LinkedList<>();
        }
        for (final String t: args) {
            if (t.trim().isEmpty()) {
                args.remove(t);
            }
        }
        if (args.isEmpty()) {
            return config;
        } else {
            return args;
        }
    }

    private List<WorkspaceIdentifier> getWsBlackList(
            final List<String> args,
            final List<WorkspaceIdentifier> config) {
        if (args == null || args.isEmpty()) {
            return config;
        } else {
            return SearchToolsConfig.toWorkspaceIdentifiers(args);
        }
    }

    private void runEventGenerator(
            final PrintStream logtarget,
            final String ref,
            final boolean verbose,
            final List<WorkspaceIdentifier> wsBlackList,
            final List<String> wsTypes)
            throws EventGeneratorException {
            final WorkspaceEventGenerator gen = new WorkspaceEventGenerator.Builder(
                    new MongoDBStatusEventStorage(searchDB), workspaceDB, logtarget)
                    .withNullableRef(ref)
                    .withWorkspaceBlacklist(wsBlackList)
                    .withWorkspaceTypes(wsTypes).build();
            gen.generateEvents();
    }

    private SearchToolsConfig getConfig(final String configPath)
            throws IOException, SearchToolsConfigException {
        final Path path = Paths.get(configPath);
        final Properties p = new Properties();
        p.load(Files.newInputStream(path));
        return SearchToolsConfig.from(p);
    }
    
    private void deleteMongoDB() {
        out.println("Deleting Mongo database: " + searchDB.getName());
        //TODO CODE just remove all items from collections rather than dropping db, preserves indexes
        searchDB.drop();
    }
    
    //TODO NOW delete elastic indexes
    private void deleteAllElasticIndices(
            final HttpHost esHostPort,
            final String esUser,
            final String esPassword,
            final String prefix) throws IOException {
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort, null);
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        for (String indexName : esStorage.listIndeces()) {
            if (indexName.startsWith(prefix)) {
                out.println("Deleting Elastic index: " + indexName);
                esStorage.deleteIndex(indexName);
            }
        }
    }

    private void usage(final JCommander jc) {
        final StringBuilder sb = new StringBuilder();
        jc.usage(sb);
        out.println(sb.toString());
    }
    
    private void printError(final Throwable e, final boolean verbose) {
        printError("Error", e, verbose);
    }
    
    private void printError(
            final String msg,
            final Throwable e,
            final boolean verbose) {
        err.println(msg + ": " + e.getMessage());
        if (verbose) {
            e.printStackTrace(err);
        }
    }
    
    private class Args {
        @Parameter(names = {"-h", "--help"}, help = true,
                description = "Display help.")
        private boolean help;
        
        @Parameter(names = {"-v", "--verbose"},
                description = "Show error stacktraces.")
        private boolean verbose;
        
        @Parameter(names = {"-c", "--config"}, required = true,
                description = "Path to the search tools config file " +
                        "(example file is search_tools.cfg.example).")
        private String configPath;
        
        @Parameter(names = {"-r", "--ref"}, description = "A workspace style (e.g. 1/2/3) ref " +
                "to be used with the -w command. " +
                "Only absolute refs are accepted. If omitted, creation events for all objects " +
                "in all workspaces will be submitted to search. A specific workspace, object, or " +
                "object version can be specified by providing some or all of the ref.")
        private String ref;
        
        @Parameter(names = {"-b", "--ws-blacklist"}, description =
                "A comma delimited list of workspace ids or names to ignore. Setting this " +
                "option overrides the blacklist in the config file.")
        private List<String> wsBlacklist;
        
        @Parameter(names = {"-t", "--ws-types"}, description =
                "A comma delimited list of workspace types to process. A prefix for each type " +
                "name may be provided. Setting this " +
                "option overrides the type list in the config file.")
        private List<String> wsTypes;
        
        @Parameter(names = {"-x", "--drop-databases"}, description =
                "Permanently, and without warning, drop the search event database and " +
                "elasticsearch indexes. Clears all data from the search system. " +
                "WARNING: there is no warning or double check for this command. Data will " +
                "be dropped immediately and will be unrecoverable.")
        private boolean dropDB;
        
        @Parameter(names = {"-w", "--generate-workspace-events"}, description =
                "Generate events for all objects in the workspace service database. " +
                "Can be used with -r to specify particular workspaces, objects, or versions.")
        private boolean genWSEvents;
    }
}
