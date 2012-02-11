package com.cloudera.fbus;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.message.MessageBuilder;
import org.springframework.util.Assert;

/**
 * A Spring Integration end point suitable for delivering messages with a
 * {@link File} payload to the Hadoop Distributed File System.
 * 
 */
public class HDFSDirectoryDestination {

  protected static final String tmpSuffix = ".tmp";
  protected static final Logger logger = LoggerFactory
      .getLogger(HDFSDirectoryDestination.class);

  protected File directory;
  protected FileSystem fileSystem;

  public HDFSDirectoryDestination() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
  }

  public void deliver(File file) throws DeliveryException {
    Path source;
    Path destinationTmp;
    Path destination;

    Assert.notNull(file, "File may not be null");
    Assert.isTrue(file.canRead(), "File " + file
        + " does not exist or is unreadable");

    
    destinationTmp = new Path(directory.getPath(), file.getName() + tmpSuffix);
    destination = new Path(directory.getPath(), file.getName());

    try {
      copyFromLocalFile(file, destinationTmp);

      if (!fileSystem.rename(destinationTmp, destination)) {
        if (!fileSystem.delete(destinationTmp, false)) {
          logger
              .warn(
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
          logger
              .error(
                  "Unable to delete file {} after moving it to HDFS @ {}. File may be retransfered!",
                  file, destination);
        }
      }
    } catch (Throwable e) {
      throw DeliveryException.newWith(MessageBuilder.withPayload(file).build(),
          e);
    }
  }

  protected void copyFromLocalFile(File file, Path destinationTmp) throws IOException {
	Path source;
	source = new Path(file.getPath());
    fileSystem.copyFromLocalFile(false, source, destinationTmp);
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

}
