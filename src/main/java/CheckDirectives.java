import com.sun.istack.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.TreeSet;

class CheckDirectives {
    private Path path;
    private Configuration conf;
    private DistributedFileSystem dfs;
    private String poolName;
    private boolean showFlag;

    CheckDirectives(Path path, String poolName, Configuration conf, boolean showFlag) throws IOException {
        this.path = path;
        this.conf = conf;
        this.poolName = poolName;
        this.showFlag = showFlag;
        dfs = (DistributedFileSystem) FileSystem.get(conf);
    }

    void check() {
        TreeSet<FileStatus> fsSet = new TreeSet<>();
        TreeSet<CacheInfo> directiveSet = new TreeSet<>();

        try {

            listDFSFiles(path, fsSet);
            listDirectives(poolName, directiveSet);
            Iterator<FileStatus> fileStatusIterator = fsSet.iterator();
            Iterator<CacheInfo> cacheInfoIterator = directiveSet.iterator();

            if (!fsSet.isEmpty() && !directiveSet.isEmpty()) {
                if (fsSet.size() != directiveSet.size()) {
                    System.out.println("[WARN] Cache directives not match hdfs files!");
                }

                for (int i=0; i<(fsSet.size() >= directiveSet.size() ? directiveSet.size() : fsSet.size()) && fileStatusIterator.hasNext() && cacheInfoIterator.hasNext(); i++) {
                        FileStatus fileStatus = fileStatusIterator.next();
                        CacheInfo cacheInfo = cacheInfoIterator.next();
                        Path fileStatusPath = fileStatus.getPath();
                        Path cacheInfoPath = cacheInfo.getInfo().getPath();

                        if (fileStatusPath.equals(cacheInfoPath)) {
                            if (cacheInfo.getStats().getFilesCached() != cacheInfo.getStats().getFilesNeeded()) {
//                                if (cacheInfo.getStats().getBytesCached() != 0)
                                System.out.printf("[WARN] Incomplete cache. Patch: %s, Cached: %3.2f%%\n",
                                    fileStatusPath.toString(),
                                        Long.valueOf(cacheInfo.getStats().getBytesCached()).floatValue() /
                                                Long.valueOf(cacheInfo.getStats().getBytesNeeded()).floatValue() * 100
                                );
                            } else {
                                if (showFlag) {
                                    System.out.printf("[INFO] Complete cache. Path: %s\n", fileStatusPath.toString());
                                }
                            }
                        } else {
                            while (!fileStatusPath.equals(cacheInfoPath)) {
                                fileStatusPath = fileStatusIterator.next().getPath();
                                if (fsSet.size() >= directiveSet.size()) {
                                    fileStatusPath = fileStatusIterator.next().getPath();
                                    System.out.println("[WARN] Path not found in directives: " + fileStatusPath);
                                } else {
                                    cacheInfoPath = cacheInfoIterator.next().getInfo().getPath();
                                    System.out.println("[WARN] Directive not found in files: " + cacheInfoPath);
                                }
                            }
                        }
                }
                while (fileStatusIterator.hasNext()) {
                    System.out.println("[WARN] Path not found in directives: " + fileStatusIterator.next().getPath());
                }
                while (cacheInfoIterator.hasNext()) {
                    System.out.println("[WARN] Directive not found in files: " + cacheInfoIterator.next().getInfo().getPath());
                }
            }

        } catch (IOException e) {
            e .printStackTrace();
        }
    }

    private void listDFSFiles(Path path, TreeSet<FileStatus> fsSet) throws IOException {
        FileStatus[] fileStatuses = dfs.listStatus(path);
        for (FileStatus fileStatus: fileStatuses) {
            if (fileStatus.isFile())
                fsSet.add(fileStatus);
            else
                listDFSFiles(fileStatus.getPath(), fsSet);
        }
    }

    private void listDirectives(String poolName, TreeSet<CacheInfo> directiveSet) throws IOException {
        CacheDirectiveInfo info = new CacheDirectiveInfo.Builder()
                .setPool(poolName)
                .build();
        RemoteIterator<CacheDirectiveEntry> cacheDirectiveEntryRemoteIterator = dfs.listCacheDirectives(info);
        while (cacheDirectiveEntryRemoteIterator.hasNext()) {
            CacheDirectiveEntry entry = cacheDirectiveEntryRemoteIterator.next();
            directiveSet.add(new CacheInfo(entry.getInfo(), entry.getStats()));
        }
    }

}

class CacheInfo extends CacheDirectiveEntry implements Comparable {
    CacheInfo(CacheDirectiveInfo info, CacheDirectiveStats stats) {
        super(info, stats);
    }

    @Override
    public int compareTo(Object o) {
        CacheInfo cacheInfo = (CacheInfo) o;
        return this.getInfo().getPath().compareTo(cacheInfo.getInfo().getPath());
    }
}