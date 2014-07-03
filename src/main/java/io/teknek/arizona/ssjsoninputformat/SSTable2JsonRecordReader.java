package io.teknek.arizona.ssjsoninputformat;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;


public class SSTable2JsonRecordReader extends LineRecordReader {

  public SSTable2JsonRecordReader(Configuration arg0, FileSplit arg1, byte[] arg2)
          throws IOException {
    super(arg0, arg1, arg2);
  }

  public SSTable2JsonRecordReader(Configuration job, FileSplit split) throws IOException {
    super(job, split);
  }

  public SSTable2JsonRecordReader(InputStream in, long offset, long endOffset, Configuration job,
          byte[] recordDelimiter) throws IOException {
    super(in, offset, endOffset, job, recordDelimiter);
  }

  public SSTable2JsonRecordReader(InputStream in, long offset, long endOffset, Configuration job)
          throws IOException {
    super(in, offset, endOffset, job);
  }

  public SSTable2JsonRecordReader(InputStream in, long offset, long endOffset, int maxLineLength,
          byte[] recordDelimiter) {
    super(in, offset, endOffset, maxLineLength, recordDelimiter);

  }

  public SSTable2JsonRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
    super(in, offset, endOffset, maxLineLength);
  }

  @Override
  public synchronized boolean next(LongWritable arg0, Text line) throws IOException {
    boolean res = super.next(arg0, line);
    if (line.charAt(0) == '['){
      res = super.next(arg0, line);
    }
    if (line.charAt(0) == ']'){
      res = super.next(arg0, line);
    }
    if (line.getLength() > 0 && line.getBytes()[line.getLength()-1]==','){
      line.set( line.getBytes(),0, line.getLength()-2);
    }
    if (res == false){
      return false;
    }
    
    return res;
  }
  
}
