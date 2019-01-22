import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestRCache {
    private Configuration conf;

    @Before
    public void Setup() {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
    }

    @Test
    public void testCheckDirectives() throws IOException {
        new CheckDirectives(new Path("/test"), "benchmarks", conf, true).check();
    }

    @Test
    public void flatCompute() {
        long i = 294132;
        long j = 147066;
        System.out.printf("%3.2f", Long.valueOf(j).floatValue() / Long.valueOf(i).floatValue());
    }
}
