package com.cloudera.fbus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.message.MessageBuilder;
import org.springframework.util.Assert;

/**
 * A Spring Integration end point suitable for delivering messages with a
 * {@link File} payload to the Hadoop Distributed File System.
 * 
 */
public class HDFSSequenceFileDirectoryDestination extends HDFSDirectoryDestination{

  protected CompressionCodec compressionCodec = new SnappyCodec(); //Snappy is the default compression

  public HDFSSequenceFileDirectoryDestination() throws IOException {
	super();
  }

  @Override
  protected void copyFromLocalFile(File file, Path destinationTmp) throws IOException {
	SequenceFile.Writer writer = null;
	BufferedReader reader = null;
	  
	try{
	  writer = SequenceFile.createWriter(fileSystem, fileSystem.getConf(), destinationTmp, NullWritable.class, Text.class, SequenceFile.CompressionType.BLOCK, compressionCodec);
	  reader = new BufferedReader(new FileReader(file));
	  
	  String line = null;
	  Text text = new Text();
	  while((line = reader.readLine()) != null){
	    text.set(line);
	    writer.append(NullWritable.get(), text);	
	  }
	}catch(Exception e){
		throw new IOException(e);
	}finally
	{   
	  if (writer != null)
	  {
	    writer.close();
	  }
	  if (reader != null)
	  {
  		reader.close();
	  }
    }
  }

  public CompressionCodec getCompressionCodec()
  {
	  return compressionCodec;
  }
  
  public void setCompressionCodec(CompressionCodec compressionCodec)
  {
	  this.compressionCodec = compressionCodec; 
  }

}
