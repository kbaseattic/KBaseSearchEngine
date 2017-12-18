package kbasesearchengine.system;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** A utility class for listing directories and opening and testing files. The purpose of this
 * class is to allow mocking file interactions for testing {@link TypeFileStorage}.
 * @author gaprice@lbl.gov
 *
 */
public class FileLister {
    
    /** Lists the contents of a directory. Wraps {@link Files#list(Path)}.
     * @param directory the directory to list.
     * @return an iterable containing the contents of the directory.
     * @throws IOException if an IO error occurs.
     */
    public Iterable<Path> list(final Path directory) throws IOException {
        // this is gross, but works. https://stackoverflow.com/a/20130475/643675
        return (Iterable<Path>) Files.list(directory)::iterator;
    }
    
    /** Check if a file is a regular file. Wraps
     * {@link Files#isRegularFile(Path, java.nio.file.LinkOption...)}.
     * @param file the file to check.
     * @return true if the file is a regular file.
     */
    public boolean isRegularFile(final Path file) {
        return Files.isRegularFile(file);
    }
    
    /** Open a new input stream for a file. Wraps
     * {@link Files#newInputStream(Path, java.nio.file.OpenOption...)}
     * @param file the file to open.
     * @return a new input stream for the file.
     * @throws IOException if an IO error occurs.
     */
    public InputStream newInputStream(final Path file) throws IOException {
        return Files.newInputStream(file);
    }

}
