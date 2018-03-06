package kbasesearchengine.tools;

import static kbasesearchengine.tools.Utils.nonNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;

import kbasesearchengine.common.FileUtil;
import org.apache.http.HttpHost;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import kbasesearchengine.common.GUID;
import kbasesearchengine.events.exceptions.IndexingException;
import kbasesearchengine.events.handler.CloneableWorkspaceClientImpl;
import kbasesearchengine.events.handler.EventHandler;
import kbasesearchengine.events.handler.WorkspaceEventHandler;
import kbasesearchengine.events.storage.MongoDBStatusEventStorage;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.events.storage.StorageInitException;
import kbasesearchengine.main.LineLogger;
import kbasesearchengine.main.SearchVersion;
import kbasesearchengine.main.Stoppable;
import kbasesearchengine.main.GitInfo;
import kbasesearchengine.main.IndexerCoordinator;
import kbasesearchengine.main.IndexerWorker;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.search.ElasticIndexingStorage;
import kbasesearchengine.search.IndexingStorage;
import kbasesearchengine.system.FileLister;
import kbasesearchengine.system.ObjectTypeParsingRulesFileParser;
import kbasesearchengine.system.TypeFileStorage;
import kbasesearchengine.system.TypeMappingParser;
import kbasesearchengine.system.TypeParseException;
import kbasesearchengine.system.TypeStorage;
import kbasesearchengine.system.YAMLTypeMappingParser;
import kbasesearchengine.tools.SearchToolsConfig.SearchToolsConfigException;
import kbasesearchengine.tools.WorkspaceEventGenerator.Builder;
import kbasesearchengine.tools.WorkspaceEventGenerator.EventGeneratorException;
import us.kbase.auth.AuthConfig;
import us.kbase.auth.AuthException;
import us.kbase.auth.AuthToken;
import us.kbase.auth.ConfigurableAuthService;
import us.kbase.common.service.UnauthorizedException;
import us.kbase.workspace.WorkspaceClient;

/** Tools for working with Search. Note that this CLI is designed against the search prototype
 * event listener in the workspace service, and may need changes if the event listener is changed
 * or removed.
 * @author gaprice@lbl.gov
 *
 */
public class SearchTools {
    
    //TODO TEST
    
    private static final String NAME = "search_tools";
    private static final int MAX_Q_SIZE = 10000;
    private static final GitInfo GIT = new GitInfo();
    
    /* The maximum number of objects to index in the search system at once. With the 18/2/23
     * implementation of ElasticSearch with a 10m load timeout, more than ~100K subobjects causes
     * a timeout (note this number and the timeout might require a bit of tweaking to completely
     * eliminate timeout errors).
     * 
     * Exceeding this number of object in a load will cause a failure after the parse step, so
     * the ElasticSearch load isn't even attempted.
     */
    private static final int MAX_OBJECTS_PER_LOAD = 100_000;

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
    private IndexingStorage indexStore = null;

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
        if (a.version) {
            printVer();
            return 0;
        }
        if (a.help) {
            usage(jc);
            return 0;
        }
        // can't be empty string since it's a command line param
        final boolean startWorker = a.startWorker != null;
        if ((a.startCoordinator ? 1 : 0) + 
                (a.genWSEvents ? 1 : 0) +
                (startWorker ? 1 : 0) > 1) {
            printError("Can only run one of the coordinator, event generator, or a worker.");
            return 1;
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
        boolean noCommand = true; // this seems dumb...
        if (a.specPath != null) {
            try {
                printVer();
                new MinimalSpecGenerator().generateMinimalSearchSpec(
                        Paths.get(a.specPath), a.storageType, a.searchType, a.storageObjectType);
                noCommand = false;
            } catch (IllegalArgumentException | IOException e) {
                printError(e, a.verbose);
                return 1;
            }
        }
        try {
            setUpMongoDBs(cfg, a.genWSEvents, a.dropDB || a.startCoordinator || startWorker);
            setUpElasticSearch(cfg, a.dropDB || startWorker);
        } catch (MongoException | IOException e) {
            printError(e, a.verbose);
            return 1;
        }
        if (a.dropDB) {
            try {
                deleteMongoDB();
                out.println("Deleting ElasticSearch indexes");
                indexStore.dropData();
                noCommand = false;
            } catch (MongoException | IOException e) {
                printError(e, a.verbose);
                return 1;
            }
        }
        if (a.startCoordinator) {
            try {
                printVer();
                final IndexerCoordinator coord = runCoordinator(cfg, out, err);
                noCommand = false; 
                waitForReturn(coord);
            } catch (StorageInitException | IndexingException | InterruptedException e) {
                printError(e, a.verbose);
                return 1;
            }
        }
        if (startWorker) {
            try {
                printVer();
                final IndexerWorker work = runWorker(cfg, a.startWorker, out, err);
                noCommand = false;
                waitForReturn(work);
            } catch (IOException | AuthException | ObjectParseException | TypeParseException |
                    UnauthorizedException | StorageInitException | IllegalArgumentException |
                    InterruptedException e) {
                printError(e, a.verbose);
                return 1;
            }
        }
        if (a.genWSEvents) {
            try {
                printVer();
                runEventGenerator(
                        out,
                        a.ref,
                        a.lastVersionOnly,
                        getWsBlackList(a.wsBlacklist, cfg.getWorkspaceBlackList()),
                        getWsTypes(a.wsTypes, cfg.getWorkspaceTypes()),
                        cfg.getWorkerCodes());
                noCommand = false;
            } catch (EventGeneratorException | StorageInitException e) {
                printError(e, a.verbose);
                return 1;
            }
        }
        if (noCommand) {
            usage(jc);
        }
        return 0;
    }
    
    public static class MinimalSpecGenerator {
        
        private static final String SPEC_TEMPLATE_FILE = "search_spec.yaml.template";
        private final MustacheFactory mf = new DefaultMustacheFactory();
        
        private final String template;
        
        public MinimalSpecGenerator() throws IOException {
            template = loadTemplate();
        }
        
        private String loadTemplate() throws IOException {
            final InputStream is = getClass().getResourceAsStream(SPEC_TEMPLATE_FILE);
            final Scanner s = new Scanner(is);
            s.useDelimiter("\\A");
            final String template = s.next();
            s.close();
            return template;
        }
    
        public void generateMinimalSearchSpec(
                final Path specPath,
                final String storageType,
                final String searchType,
                final String storageObjectType)
                throws IOException {
            if (specPath == null) {
                throw new IllegalArgumentException("spec path is missing");
            }
            if (Utils.isNullOrEmpty(storageType)) {
                throw new IllegalArgumentException("storage type must be provided in arguments");
            }
            if (Utils.isNullOrEmpty(searchType)) {
                throw new IllegalArgumentException("search type must be provided in arguments");
            }
            if (Utils.isNullOrEmpty(storageObjectType)) {
                throw new IllegalArgumentException(
                        "storage object type must be provided in arguments");
            }
            final StringWriter sw = new StringWriter();
            mf.compile(new StringReader(template), "template").execute(sw, ImmutableMap.of(
                    "search_type", searchType,
                    "storage_type", storageType,
                    "storage_object_type", storageObjectType));
            Files.write(specPath, sw.toString().getBytes());
        }
    }
    
    private void waitForReturn(final Stoppable stoppable) throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            
            @Override
            public void run() {
                System.out.println("Waiting for task to stop");
                try {
                    stoppable.stop(10 * 60 * 1000); // 10 mins
                } catch (InterruptedException e) {
                    // do nothing, things are going down anyway
                }
            }
        });
        stoppable.awaitShutdown();
    }
    
    private IndexerCoordinator runCoordinator(
            final SearchToolsConfig cfg,
            final PrintStream logTarget,
            final PrintStream errTarget)
            throws StorageInitException, InterruptedException, IndexingException {
        final LineLogger logger = buildLogger(logTarget, errTarget);
        
        final StatusEventStorage storage = new MongoDBStatusEventStorage(searchDB);
        
        final IndexerCoordinator coord = new IndexerCoordinator(storage, logger, MAX_Q_SIZE);
        coord.startIndexer();
        return coord;
    }
    
    private IndexerWorker runWorker(
            final SearchToolsConfig cfg,
            final String id,
            final PrintStream logTarget,
            final PrintStream errTarget)
            throws IOException, AuthException, ObjectParseException, TypeParseException,
                UnauthorizedException, StorageInitException {
        final AuthToken kbaseIndexerToken = getIndexerToken(cfg);
        final File tempDir = new File(cfg.getTempDir());
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        final LineLogger logger = buildLogger(logTarget, errTarget);
        
        final Map<String, TypeMappingParser> parsers = ImmutableMap.of(
                "yaml", new YAMLTypeMappingParser());
        final Path typesDir = Paths.get(cfg.getTypesDirectory());
        final Path mappingsDir = Paths.get(cfg.getTypeMappingsDirectory());
        final TypeStorage ss = new TypeFileStorage(typesDir, mappingsDir,
                new ObjectTypeParsingRulesFileParser(), parsers, new FileLister(), logger);
        
        final StatusEventStorage storage = new MongoDBStatusEventStorage(searchDB);
        
        final WorkspaceClient wsClient = new WorkspaceClient(
                cfg.getWorkspaceURL(), kbaseIndexerToken);
        wsClient.setIsInsecureHttpConnectionAllowed(true); //TODO SEC only do if http
        final EventHandler weh = new WorkspaceEventHandler(
                new CloneableWorkspaceClientImpl(wsClient));
        
        final IndexerWorker wrk = new IndexerWorker(
                getID(id), Arrays.asList(weh), storage, indexStore, ss, tempDir, logger,
                cfg.getWorkerCodes(), MAX_OBJECTS_PER_LOAD);
        wrk.startIndexer();
        return wrk;
    }

    private String getID(String id) {
        Utils.notNullOrEmpty(id, "id cannot be null or empty");
        id = id.trim();
        if ("-".equals(id)) {
            return UUID.randomUUID().toString();
        } else {
            return id;
        }
    }

    private LineLogger buildLogger(final PrintStream logTarget, final PrintStream errTarget) {
        
        final LineLogger logger = new LineLogger() {
            
            private String decorate(final String log) {
                final Instant now = Instant.now();
                return now.toEpochMilli() + " " + now + " " + log;
            }
            
            @Override
            public void logInfo(final String line) {
                logTarget.println(decorate(line));
            }
            @Override
            public void logError(final String line) {
                errTarget.println(decorate(line));
            }
            @Override
            public void logError(final Throwable error) {
                errTarget.print(decorate("Error: \n"));
                error.printStackTrace(errTarget);
            }
            @Override
            public void timeStat(GUID guid, long loadMs, long parseMs, long indexMs) {
            }
        };
        return logger;
    }

    private AuthToken getIndexerToken(final SearchToolsConfig cfg)
            throws IOException, AuthException {
        final AuthConfig c = new AuthConfig();
        if (cfg.isAllowInsecureAuthURL()) {
            c.withAllowInsecureURLs(true);
        }
        try {
            c.withKBaseAuthServerURL(cfg.getAuthURL());
        } catch (URISyntaxException e) {
            throw new RuntimeException("This should virtually never happen", e);
        }
        final ConfigurableAuthService auth = new ConfigurableAuthService(c);
        final AuthToken kbaseIndexerToken = auth.validateToken(cfg.getIndexerToken());
        return kbaseIndexerToken;
    }

    private void setUpElasticSearch(final SearchToolsConfig cfg, final boolean dontSkip)
            throws IOException {
        if (!dontSkip) {
            return;
        }
        final HttpHost esHostPort = new HttpHost(cfg.getElasticHost(), cfg.getElasticPort());
        final File tempSubDir = FileUtil.getOrCreateSubDir(
                new File(cfg.getTempDir()), "esbulk");
        final ElasticIndexingStorage esStorage = new ElasticIndexingStorage(
                esHostPort, tempSubDir);
        if (cfg.getElasticUser().isPresent()) {
            esStorage.setEsUser(cfg.getElasticUser().get());
            esStorage.setEsPassword(new String(cfg.getElasticPassword().get()));
        }
        esStorage.setIndexNamePrefix(cfg.getElasticNamespace() + ".");
        indexStore = esStorage;
    }

    private void setUpMongoDBs(
            final SearchToolsConfig cfg,
            final boolean workspaceAndSearch,
            final boolean search)
            throws MongoException {
        
        if (!workspaceAndSearch && !search) {
            return;
        }
        if (workspaceAndSearch) {
            setUpBothMongoDatabases(cfg);
        } else {
            setUpSearchMongoDatabase(cfg);
        }
    }

    private void setUpSearchMongoDatabase(final SearchToolsConfig cfg) {
        // should close connection, but we rely on the program exiting to do that here
        final MongoClient searchClient = getMongoClient(cfg.getSearchMongoHost(), cfg.getSearchMongoDB(),
                cfg.getSearchMongoUser(), cfg.getSearchMongoPwd());
        searchDB = searchClient.getDatabase(cfg.getSearchMongoDB());
    }

    private void setUpBothMongoDatabases(final SearchToolsConfig cfg) {
        // should close connection, but we rely on the program exiting to do that here
        final MongoClient searchClient;
        final MongoClient wsClient;
        if (cfg.getSearchMongoHost().equals(cfg.getWorkspaceMongoHost())) {
            final List<MongoCredential> creds = new LinkedList<>();
            addMongoCred(cfg.getSearchMongoDB(), cfg.getSearchMongoUser(),
                    cfg.getSearchMongoPwd(), creds);
            addMongoCred(cfg.getWorkspaceMongoDB(), cfg.getWorkspaceMongoUser(),
                    cfg.getWorkspaceMongoPwd(), creds);
            searchClient = new MongoClient(new ServerAddress(cfg.getSearchMongoHost()), creds);
            wsClient = searchClient;
        } else {
            searchClient = getMongoClient(cfg.getSearchMongoHost(), cfg.getSearchMongoDB(),
                    cfg.getSearchMongoUser(), cfg.getSearchMongoPwd());
            wsClient = getMongoClient(cfg.getWorkspaceMongoHost(), cfg.getWorkspaceMongoDB(),
                    cfg.getWorkspaceMongoUser(), cfg.getWorkspaceMongoPwd());
            
        }
        searchDB = searchClient.getDatabase(cfg.getSearchMongoDB());
        workspaceDB = wsClient.getDatabase(cfg.getWorkspaceMongoDB());
    }

    private MongoClient getMongoClient(
            final String host,
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd) {
        return new MongoClient(new ServerAddress(host), getMongoCred(db, user, pwd));
    }

    private List<MongoCredential> getMongoCred(
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd) {
        final List<MongoCredential> creds = new LinkedList<>();
        addMongoCred(db, user, pwd, creds);
        return creds;
    }

    private void addMongoCred(
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
            final boolean lastVersionOnly,
            final List<WorkspaceIdentifier> wsBlackList,
            final List<String> wsTypes,
            final Set<String> workerCodes)
            throws EventGeneratorException, StorageInitException {
        final Builder gen = new WorkspaceEventGenerator.Builder(
                new MongoDBStatusEventStorage(searchDB), workspaceDB, logtarget)
                .withNullableRef(ref)
                .withWorkspaceBlacklist(wsBlackList)
                .withWorkerCodes(workerCodes)
                .withLastVersionOnly(lastVersionOnly)
                .withWorkspaceTypes(wsTypes);
        gen.build().generateEvents();
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
    
    private void usage(final JCommander jc) {
        final StringBuilder sb = new StringBuilder();
        jc.usage(sb);
        out.println(sb.toString());
    }
    
    private void printVer() {
        out.println(String.format("Software version %s (commit %s)", SearchVersion.VERSION,
                GIT.getGitCommit()));
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
    
    private void printError(final String msg) {
        err.println(msg);
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
        
        @Parameter(names = {"--destroy-all-databases"}, description =
                "Permanently, and without warning, drop the search event database and " +
                "elasticsearch indexes. Clears all data from the search system. " +
                "WARNING: there is no warning or double check for this command. Data will " +
                "be dropped immediately and will be unrecoverable.")
        private boolean dropDB;
        
        @Parameter(names = {"-s", "--start-coordinator"}, description =
                "Start the indexer coordinator. Only one coordinator may be run per search " +
                "instance, but many workers may be run.")
        private boolean startCoordinator;
        
        @Parameter(names = {"-k", "--start-worker"}, description =
                "Start an indexer worker with the provided id. At any given time, all workers " +
                "MUST have unique ids. Set the id to '-' to generate a random id.")
        private String startWorker;
        
        @Parameter(names = {"-w", "--generate-workspace-events"}, description =
                "Generate events for all objects in the workspace service database. " +
                "Can be used with -r to specify particular workspaces, objects, or versions.")
        private boolean genWSEvents;
        
        @Parameter(names = {"--spec"}, description =
                "Generate a minimal search transformation specification at the provided file " +
                "location. The --storage-type, --search-type, and --storage-obj-type arguments " +
                "are required.")
        private String specPath;
        
        @Parameter(names = {"--search-type"}, description =
                "The search type with which to initialize a new search transformation spec. See " +
                "--spec.")
        private String searchType;
        
        @Parameter(names = {"--storage-type"}, description =
                "The storage type (e.g. 'WS') with which to initialize a new search " +
                "transformation spec. See --spec.")
        private String storageType;
        
        @Parameter(names = {"--storage-object-type"}, description =
                "The type of the object in a storage system (e.g. 'KBaseGenomes.Genome') with " +
                "which to initialize a new search transformation spec. See --spec.")
        private String storageObjectType;
        
        @Parameter(names = {"--last-version-only"}, description = 
                "When generating events, only generate events for the last version of each " +
                "object. This parameter is ignored if a full ref including a version is " +
                "provided in the ref argument.")
        private boolean lastVersionOnly;
                        
        @Parameter(names = {"--version"}, description = "Print the software version and exit")
        private boolean version;
    }
}
