package kbaserelationengine.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

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
        // these lines are only tested manually, so don't make changes without testing manually.
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
    
    private static void nonNull(final Object o, final String message) {
        if (o == null) {
            throw new NullPointerException(message);
        }
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
            printError(e, a);
            return 1;
        }
        if (a.help) {
            usage(jc);
            return 0;
        }
        final Map<String, String> cfg;
        try {
            cfg = getConfig(a);
        } catch (NoSuchFileException e) {
            printError("No such file", e, a);
            return 1;
        } catch (AccessDeniedException e) {
            printError("Access denied", e, a);
            return 1;
        } catch (IOException e) {
            printError(e, a);
            return 1;
        }
        out.println(cfg);
        return 0;
    }
    
    private Map<String, String> getConfig(final Args a) throws IOException {
        final Path path = Paths.get(a.configPath);
        final Properties p = new Properties();
        p.load(Files.newInputStream(path));
        final Map<String, String> cfg = new HashMap<>();
        for (final Entry<Object, Object> e: p.entrySet()) {
            cfg.put((String) e.getKey(), (String) e.getValue());
        }
        return cfg;
    }

    private void usage(final JCommander jc) {
        final StringBuilder sb = new StringBuilder();
        jc.usage(sb);
        out.println(sb.toString());
    }
    
    private void printError(final Throwable e, final Args a) {
        printError("Error", e, a);
    }
    
    private void printError(
            final String msg,
            final Throwable e,
            final Args a) {
        err.println(msg + ": " + e.getMessage());
        if (a.verbose) {
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
        
    }
}
