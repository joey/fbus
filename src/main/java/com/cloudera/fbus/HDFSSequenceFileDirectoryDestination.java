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
public class HDFSSequenceFileDirectoryDestination {

  private static final String tmpSuffix = ".tmp";
  private static final Logger logger = LoggerFactory.getLogger(HDFSDirectoryDestination.class);
  private File directory;
  private FileSystem fileSystem;
  private CompressionCodec compressionCodec = new SnappyCodec(); //Snappy is the default compression
  private Configuration config;

  public HDFSSequenceFileDirectoryDestination() throws IOException {
    config = new Configuration();
    fileSystem = FileSystem.get(config);
  }

  public void deliver(File file) throws DeliveryException {
    Path source;
    Path destinationTmp;
    Path destination;

    Assert.notNull(file, "File may not be null");
    Assert.isTrue(file.canRead(), "File " + file
        + " does not exist or is unreadable");

    source = new Path(file.getPath());
    destinationTmp = new Path(directory.getPath(), source.getName() + tmpSuffix);
    destination = new Path(directory.getPath(), source.getName());

    try {

      copyFromLocalToSeqFile(file, destinationTmp);

      if (!fileSystem.rename(destinationTmp, destination)) {
        if (!fileSystem.delete(destinationTmp, false)) {
          logger.warn(
              "Failed to clean up temporary file at {} after rename failed. File will be retried.",
              destinationTmp);
        }

        throw new IOException("Can't rename " + destinationTmp + " to "
            + destination);
      } else {
        if (!file.delete()) {
          /*
           * Purposefully do not throw an exception here as it would cause retry
           * as well. -esammer
           */
          logger.error(
              "Unable to delete file {} after moving it to HDFS @ {}. File may be retransfered!",
              file, destination);
        }
      }
    } catch (Throwable e) {
      throw DeliveryException.newWith(MessageBuilder.withPayload(file).build(),
          e);
    }
  }

  private void copyFromLocalToSeqFile(File file, Path destinationTmp) throws IOException, FileNotFoundException {
    SequenceFile.Writer writer = null;
    BufferedReader reader = null;


    try {
      writer = SequenceFile.createWriter(fileSystem, config, destinationTmp, NullWritable.class, Text.class, SequenceFile.CompressionType.BLOCK, compressionCodec);
      reader = new BufferedReader(new FileReader(file));

      String line = null;
      Text text = new Text();
      while ((line = reader.readLine()) != null) {
        text.set(line);
        writer.append(NullWritable.get(), text);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  public CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
  }
}
