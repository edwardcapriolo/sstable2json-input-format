package io.teknek.arizona.ssjsoninputformat;
import io.teknek.arizona.ssjsoninputformat.SSTable2JsonInputFormat;
import io.teknek.hiveunit.HiveTestService;
import io.teknek.hiveunit.builders.Row;
import static io.teknek.hiveunit.builders.File.*;

import java.io.IOException;


import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.Test;

public class ANotGreatTest extends HiveTestService {

  public ANotGreatTest() throws IOException {
    // super(LOCAL_MR,LOCAL_FS,1,1);
    super();
  }

  @Test
  public void testIt() throws Exception {
    String table = "emtpymrcar";
    Path p = new Path(this.ROOT_DIR, table);
    File(getFileSystem(), p)
    .withRow(new Row().withColumn("["))
    .withRow(new Row().withColumn("{\"key\": \"62736d697468\",\"columns\": [[\"6c6173746e616d65\",\"736d697468\",1404396845806000]]},"))
    .withRow(new Row().withColumn("{\"key\": \"6563617072696f6c6f\",\"columns\": [[\"66697273746e616d65\",\"656477617264\",1404396708566000], [\"6c6173746e616d65\",\"63617072696f6c6f\",1404396801537000]]}"))
    .build();

    JobConf jc = this.createJobConf();
    jc.setInputFormat(SSTable2JsonInputFormat.class);
    jc.setMapOutputKeyClass(LongWritable.class);
    jc.setMapOutputValueClass(Text.class);
    jc.setMapperClass(ToStringMapper.class);
    FileInputFormat.setInputPaths(jc, p);
    jc.setOutputFormat(NullOutputFormat.class);

    JobClient client = new JobClient(jc);
    client.runJob(jc);

  }

  public static class ToStringMapper implements Mapper<LongWritable,Text,Text,Text> {

    @Override
    public void map(LongWritable arg0, Text arg1, OutputCollector<Text, Text> arg2, Reporter arg3)
            throws IOException {
      System.out.println(arg1);
      
    }

    @Override
    public void configure(JobConf arg0) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }
    
  }
}
