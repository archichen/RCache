import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;

import java.io.IOException;
import java.net.URI;
import java.util.PriorityQueue;

public class RCache {
    private static int maxDepth = -1;
    private static String fPath;
    private static FileSystem fileSystem;
    private static Configuration conf = new Configuration();
    private static String poolName;
    private static int replication = 1;
    private static boolean isVerbose = false;
    private static boolean showFlag = false;

    private RCache() throws IOException {
        PriorityQueue<FileStatus> uncachedPaths = new PriorityQueue<>();
        DistributedFileSystem hdfs = (DistributedFileSystem) FileSystem.get(conf);
        RemoteIterator<CachePoolEntry> cachePoolEntryRemoteIterator = hdfs.listCachePools();

        try {
            boolean hasPool = false;
            while (cachePoolEntryRemoteIterator.hasNext()) {
                CachePoolEntry cachePoolEntry = cachePoolEntryRemoteIterator.next();
                if (cachePoolEntry.getInfo().getPoolName().equals(poolName)) {
                    hasPool = true;
                }
            }
            if (!hasPool) {
                System.out.println("No pool exists named " + poolName);
                System.exit(1);
            }

            uncachedPaths.add(fileSystem.getFileStatus(new Path(fPath)));
            while (!uncachedPaths.isEmpty()) {
                FileStatus fs = uncachedPaths.poll();
                FileStatus[] childs = fileSystem.listStatus(fs.getPath());
                if (childs.length == 0) continue;
                for (FileStatus c : childs) {
                    if (c.isDirectory()) {
                        uncachedPaths.add(c);
                    } else {
                        CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
                        builder.setPath(c.getPath());
                        builder.setPool(poolName);
                        builder.setReplication((short) replication);
                        if (isVerbose) System.out.println("Cached: " + c.getPath().toString());
                        hdfs.addCacheDirective(builder.build());
                    }
                }
            }

            System.out.println("Cache done.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();

        options.addOption("d", "directory", true, "Root path which would be cached.");
        options.addOption("p", "pool", true, "Cache pool name.");
        options.addOption("r", "replication", true, "Amount of cache replication.");
        options.addOption("v", "verbose", false, "Verbosely list files processed.");
        options.addOption("h", "help", false, "Get help.");
        options.addOption("c", "check", true, "Check directives whether fully cached or not.");
        options.addOption("show", "show", false, "This option will print all fully cached directives.");

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.getOptions().length == 0) {
                throw new ParseException("Must give parameters!");
            }

            if (cmd.hasOption("d") && cmd.hasOption("p")) {
                fPath = cmd.getOptionValue("d");
                URI dirURI = URI.create(fPath);
                if (dirURI.getScheme() != null) {
                    fPath = dirURI.getPath();
                }
                fileSystem = FileSystem.get(URI.create(fPath), conf);

                poolName = cmd.getOptionValue("p");
            } else if (cmd.hasOption("c") && cmd.hasOption("p")){
                showFlag = cmd.hasOption("show");
                new CheckDirectives(new Path(cmd.getOptionValue("c")), poolName, conf, showFlag).check();
                System.exit(0);
            } else {
                throw new ParseException("Missing parameters.");
            }

            if (cmd.hasOption("r")) {
                replication = Integer.parseInt(cmd.getOptionValue("r"));
            }

            if (cmd.hasOption("v")) {
                isVerbose = true;
            }

            if (cmd.hasOption("h")) {
                throw new ParseException("Help info:");
            }

            new RCache();
        } catch (ParseException pe) {
            System.out.println(pe.getMessage());
            System.out.println("Cache file recursively");
            formatter.printHelp("RCache -d <Dir> -p <Pool> [-r <Replication=1>] [-v <Verbose>] [-c <Dir>] [-show <Show=false>] [-h <Help>]", options);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
