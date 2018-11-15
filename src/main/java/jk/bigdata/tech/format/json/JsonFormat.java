package jk.bigdata.tech.format.json;

import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import jk.bigdata.tech.AdlSinkConnectorConfig;
import jk.bigdata.tech.storage.AdlStorage;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.HashMap;
import java.util.Map;

public class JsonFormat implements Format<AdlSinkConnectorConfig, String> {
  private final AdlStorage storage;
  private final JsonConverter converter;

  public JsonFormat(AdlStorage storage) {
    this.storage = storage;
    this.converter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put("schemas.cache.size",
            String.valueOf(storage.conf().get(AdlSinkConnectorConfig
                    .SCHEMA_CACHE_SIZE_CONFIG)));
    this.converter.configure(converterConfig, false);
  }

  @Override
  public RecordWriterProvider<AdlSinkConnectorConfig>
  getRecordWriterProvider() {
    return new JsonRecordWriterProvider(storage, converter);
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
