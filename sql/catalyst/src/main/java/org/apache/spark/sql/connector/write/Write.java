package org.apache.spark.sql.connector.write;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public interface Write {

  default String description() {
    return this.getClass().toString();
  }

  /**
   * Returns a {@link BatchWrite} to write data to batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#BATCH_WRITE} support in
   * its {@link Table#capabilities()}.
   */
  default BatchWrite toBatch() {
    throw new UnsupportedOperationException(description() + ": Batch write is not supported");
  }

  /**
   * Returns a {@link StreamingWrite} to write data to streaming source. By default this method
   * throws exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#STREAMING_WRITE} support
   * in its {@link Table#capabilities()}.
   */
  default StreamingWrite toStreaming() {
    throw new UnsupportedOperationException(description() + ": Streaming write is not supported");
  }
}
