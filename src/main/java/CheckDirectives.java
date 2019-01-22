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
        // 源文件和Cache directives使用TreeSet按Path排序
        TreeSet<FileStatus> fsSet = new TreeSet<>();
        TreeSet<CacheInfo> directiveSet = new TreeSet<>();

        try {
            // 获取源文件列表。
            listDFSFiles(path, fsSet);
            // 获得Directives列表
            listDirectives(poolName, directiveSet);
            Iterator<FileStatus> fileStatusIterator = fsSet.iterator();
            Iterator<CacheInfo> cacheInfoIterator = directiveSet.iterator();
            
            // 当两个列表都非空时进入程序主体
            if (!fsSet.isEmpty() && !directiveSet.isEmpty()) {
                // 如果两个列表长度不一，打印警告。（按照RCache的逻辑，正常情况下源文件和directives应该一摸一样，如果不一样，就是其中一项被修改）
                if (fsSet.size() != directiveSet.size()) {
                    System.out.println("[WARN] Cache directives not match hdfs files!");
                }
                
                // 按以长度较小的一个set作为循环基准。
                for (int i=0; i<(fsSet.size() >= directiveSet.size() ? directiveSet.size() : fsSet.size()) && fileStatusIterator.hasNext() && cacheInfoIterator.hasNext(); i++) {
                        FileStatus fileStatus = fileStatusIterator.next();
                        CacheInfo cacheInfo = cacheInfoIterator.next();
                        Path fileStatusPath = fileStatus.getPath();
                        Path cacheInfoPath = cacheInfo.getInfo().getPath();
                        
                        // 两个set的迭代器指针同时移动，正常情况下，两个指针指向的path都一样。
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
                        // 如果出现不一样，代表源文件或者directives出现被修改或者缺失。
                        } else {
                            // TODO: 这里可能会导致NullElementException异常。修复方法：判空。
                            // 但是当源文件最后一个不在cache中，cache directives的最后一个不在源文件中时，会无法判断检测哪个。修复方法：使用类包装fsSet和directiveSet，并实现Iterable和Comparable接口，然后修复程序结构。       
                            
                            // 将较长的一个set迭代器指针向下移动，直到出现与当前cache path相同的path为止。这里正常运行的情况是directives是源文件的真子集，绝大多数情况也都是这样。但是因此如果非真子集情况就会出现边界问题，待修复。
                            while (!fileStatusPath.equals(cacheInfoPath)) {
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
                // 如果两个set长度不一样，当较小的一个迭代完毕后，较长的一个还有剩余。这些剩余的部分就是未被缓存或者缺失的源文件。
                // 如果源文件set较长，则存在未被缓存的path
                while (fileStatusIterator.hasNext()) {
                    System.out.println("[WARN] Path not found in directives: " + fileStatusIterator.next().getPath());
                }
                // 如果directives的set较长，则存在源文件缺失
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
