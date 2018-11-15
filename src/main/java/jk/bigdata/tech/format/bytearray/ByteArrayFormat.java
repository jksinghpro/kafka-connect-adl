package jk.bigdata.tech.format.bytearray;

import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import jk.bigdata.tech.AdlSinkConnectorConfig;
import jk.bigdata.tech.storage.AdlStorage;
import org.apache.kafka.connect.converters.ByteArrayConverter;

import java.util.HashMap;
import java.util.Map;

public class ByteArrayFormat implements Format<AdlSinkConnectorConfig, String> {
  private final AdlStorage storage;
  private final ByteArrayConverter converter;

  public ByteArrayFormat(AdlStorage storage) {
    this.storage = storage;
    this.converter = new ByteArrayConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    this.converter.configure(converterConfig, false);
  }

  @Override
  public RecordWriterProvider<AdlSinkConnectorConfig>
  getRecordWriterProvider() {
    return new ByteArrayRecordWriterProvider(storage, converter);
  }

  @Override
  public SchemaFileReader<AdlSinkConnectorConfig, String>
  getSchemaFileReader() {
    throw new UnsupportedOperationException(
            "Reading schemas from Azure blob is not currently supported");
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException(
            "Hive integration is not currently supported in Azure blob " +
                    "Connector");
  }

}
