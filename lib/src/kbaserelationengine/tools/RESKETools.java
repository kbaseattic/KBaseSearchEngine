package kbaserelationengine.tools;

import static kbaserelationengine.tools.Utils.nonNull;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import kbaserelationengine.tools.WorkspaceEventGenerator.EventGeneratorException;
import kbaserelationengine.tools.RESKEToolsConfig.RESKEToolsConfigException;

/** Tools for working with RESKE.
 * @author gaprice@lbl.gov
 *
 */
public class RESKETools {
    
    private static final String NAME = "reske_tools";

    /** Runs the CLI.
     * @param args the program arguments.
     */
    public static void main(String[] args) {
        System.exit(new RESKETools(args, System.out, System.err).execute());
    }
    
    private final String[] args;
    private final PrintStream out;
    private final PrintStream err;

    /** Create a new CLI instance.
     * @param args the program arguments.
     * @param console the system console.
     * @param out the out printstream.
     * @param err the error printstream.
     */
    public RESKETools(
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
        final RESKEToolsConfig cfg;
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
        } catch (RESKEToolsConfigException e) {
            printError("For config file " + a.configPath, e, a.verbose);
            return 1;
        }
        return runEventGenerator(cfg, out, a.ref, a.verbose,
                getWsBlackList(a.wsBlacklist, cfg.getWorkspaceBlackList()));
    }
    
    private List<WorkspaceIdentifier> getWsBlackList(
            final List<String> args,
            final List<WorkspaceIdentifier> config) {
        if (args == null || args.isEmpty()) {
            return config;
        } else {
            return RESKEToolsConfig.toWorkspaceIdentifiers(args);
        }
    }

    private int runEventGenerator(
            final RESKEToolsConfig cfg,
            final PrintStream logtarget,
            final String ref,
            final boolean verbose,
            final List<WorkspaceIdentifier> wsBlackList) {
        try {
            final WorkspaceEventGenerator gen = new WorkspaceEventGenerator.Builder(cfg, logtarget)
                    .withNullableRef(ref).withWorkspaceBlacklist(wsBlackList).build();
            gen.generateEvents();
            gen.destroy();
        } catch (EventGeneratorException e) {
            printError(e, verbose);
            return 1;
        }
        return 0;
    }

    private RESKEToolsConfig getConfig(final String configPath)
            throws IOException, RESKEToolsConfigException {
        final Path path = Paths.get(configPath);
        final Properties p = new Properties();
        p.load(Files.newInputStream(path));
        return RESKEToolsConfig.from(p);
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
                description = "Path to the RESKE tools config file " +
                        "(example file is reske_tools.cfg.example).")
        private String configPath;
        
        @Parameter(names = {"-r", "--ref"}, description = "A workspace style (e.g. 1/2/3) ref. " +
                "Only absolute refs are accepted. If omitted, creation events for all objects " +
                "in all workspaces will be submitted to RESKE. A specific workspace, object, or " +
                "object version can be specified by providing some or all of the ref.")
        private String ref;
        
        @Parameter(names = {"-b", "--ws-blacklist"}, description =
                "A comma delimited list of workspace ids or names to ignore. Setting this " +
                "option overrides the blacklist in the config file.")
        private List<String> wsBlacklist;
        
    }
}
