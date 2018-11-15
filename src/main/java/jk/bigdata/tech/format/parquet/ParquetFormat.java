package jk.bigdata.tech.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import jk.bigdata.tech.AdlSinkConnectorConfig;
import jk.bigdata.tech.storage.AdlStorage;

public class ParquetFormat implements Format<AdlSinkConnectorConfig, String> {
  private final AdlStorage storage;
  private final AvroData avroData;

  public ParquetFormat(AdlStorage storage) {
    this.storage = storage;
    this.avroData = new AvroData(
            storage.conf().getInt(AdlSinkConnectorConfig
                    .SCHEMA_CACHE_SIZE_CONFIG)
    );
  }

  @Override
  public RecordWriterProvider<AdlSinkConnectorConfig>
  getRecordWriterProvider() {
    return new ParquetRecordWriterProvider(avroData);
  }

  @Override
  public SchemaFileReader<AdlSinkConnectorConfig, String>
  getSchemaFileReader() {
    throw new UnsupportedOperationException("Reading schemas from blob is" +
            " not currently supported");
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException(
            "Hive integration is not currently supported in blob Connector");
  }

}
