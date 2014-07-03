package io.teknek.arizona.ssjsoninputformat;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;


public class SSTable2JsonInputFormat extends FileInputFormat<LongWritable,Text> {

  public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job,
          Reporter reporter) throws IOException {
    reporter.setStatus(genericSplit.toString());
    return new SSTable2JsonRecordReader(job, (FileSplit) genericSplit);
  }


}
