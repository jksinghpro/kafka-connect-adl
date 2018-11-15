package jk.bigdata.tech;

/**
 * Created by jaskaransingh on 11/6/18.
 */

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator;
import io.confluent.connect.storage.hive.schema.TimeBasedSchemaGenerator;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import jk.bigdata.tech.format.avro.AvroFormat;
import jk.bigdata.tech.storage.AdlStorage;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class AdlSinkConnectorConfig extends StorageSinkConnectorConfig {

  public static final String ADL_DATALAKE_NAME = "adl.data.lake.name";
  public static final String ADL_CLIENT_ID = "adl.client.id";
  public static final String ADL_CLIENT_SECRET = "adl.client.secret";
  public static final String ADL_TOKEN_ENDPOINT = "adl.token.endpoint";

  public static final String AVRO_CODEC_CONFIG = "avro.codec";
  public static final String AVRO_CODEC_DEFAULT = "null";

  public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format" +
          ".bytearray.extension";
  public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";

  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG =
          "format.bytearray.separator";
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT =
          System.lineSeparator();

  private final String name;

  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;

  private final Map<String, ComposableConfig> propertyToConfig = new
          HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();

  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new
          GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new
          GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER =
          new GenericRecommender();
  private static final GenericRecommender SCHEMA_GENERATOR_CLASS_RECOMMENDER =
          new GenericRecommender();
  private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER
          = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat
          .class, AVRO_SUPPORTED_CODECS);


  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
            Arrays.<Object>asList(AdlStorage.class)
    );

    //    FORMAT_CLASS_RECOMMENDER.addValidValues(
    //        Arrays.<Object>asList(AvroFormat.class, JsonFormat.class)
    //    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
            Arrays.<Object>asList(
                    DefaultPartitioner.class,
                    HourlyPartitioner.class,
                    DailyPartitioner.class,
                    TimeBasedPartitioner.class,
                    FieldPartitioner.class
            )
    );

    SCHEMA_GENERATOR_CLASS_RECOMMENDER.addValidValues(
            Arrays.<Object>asList(
                    DefaultSchemaGenerator.class,
                    TimeBasedSchemaGenerator.class
            )
    );

  }


  public static ConfigDef newConfigDef() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
            FORMAT_CLASS_RECOMMENDER,
            AVRO_COMPRESSION_RECOMMENDER
    );
    {
      final String group = "AZ";
      int orderInGroup = 0;

      configDef.define(
              ADL_DATALAKE_NAME,
              ConfigDef.Type.STRING,
              "default",
              ConfigDef.Importance.MEDIUM,
              "The Data lake name.",
              group,
              ++orderInGroup,
              ConfigDef.Width.LONG,
              "Azure Data Lake"
      );

      configDef.define(
              ADL_CLIENT_ID,
              ConfigDef.Type.STRING,
              "default",
              ConfigDef.Importance.MEDIUM,
              "The Client Id.",
              group,
              ++orderInGroup,
              ConfigDef.Width.LONG,
              "Client Id"
      );

      configDef.define(
              ADL_CLIENT_SECRET,
              ConfigDef.Type.STRING,
              "default",
              ConfigDef.Importance.MEDIUM,
              "The Client Secret.",
              group,
              ++orderInGroup,
              ConfigDef.Width.LONG,
              "Client Secret"
      );

      configDef.define(
              ADL_TOKEN_ENDPOINT,
              ConfigDef.Type.STRING,
              "default",
              ConfigDef.Importance.MEDIUM,
              "The Token Endpoint Point.",
              group,
              ++orderInGroup,
              ConfigDef.Width.LONG,
              "Token endpoint"
      );

      /*
      configDef.define(
          AVRO_CODEC_CONFIG,
          Type.STRING,
          AVRO_CODEC_DEFAULT,
          Importance.LOW,
          "The Avro compression codec to be used for output files. Available
          values: null, "
              + "deflate, snappy and bzip2 (codec source is org.apache.avro
              .file.CodecFactory)",
          group,
          ++orderInGroup,
          Width.LONG,
          "Avro compression codec"
      );
      */

      configDef.define(
              FORMAT_BYTEARRAY_EXTENSION_CONFIG,
              ConfigDef.Type.STRING,
              FORMAT_BYTEARRAY_EXTENSION_DEFAULT,
              ConfigDef.Importance.LOW,
              String.format(
                      "Output file extension for ByteArrayFormat. " +
                              "Defaults to '%s'",
                      FORMAT_BYTEARRAY_EXTENSION_DEFAULT
              ),
              group,
              ++orderInGroup,
              ConfigDef.Width.LONG,
              "Output file extension for ByteArrayFormat"
      );

      configDef.define(
              FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG,
              ConfigDef.Type.STRING,
              // Because ConfigKey automatically trims strings, we
              // cannot set
              // the default here and instead inject null;
              // the default is applied in
              // getFormatByteArrayLineSeparator().
              null,
              ConfigDef.Importance.LOW,
              "String inserted between records for ByteArrayFormat. "
                      + "Defaults to 'System.lineSeparator()' "
                      + "and may contain escape sequences like '\\n'. "
                      + "An input record that contains the line " +
                      "separator will look like "
                      + "multiple records in the output Azure blob " +
                      "object.",
              group,
              ++orderInGroup,
              ConfigDef.Width.LONG,
              "Line separator ByteArrayFormat"
      );
    }
    return configDef;
  }

  public AdlSinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef(), props);
  }

  protected AdlSinkConnectorConfig(ConfigDef configDef, Map<String, String>
          props) {
    super(configDef, props);
    ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef
            (STORAGE_CLASS_RECOMMENDER);
    commonConfig = new StorageCommonConfig(storageCommonConfigDef,
            originalsStrings());
    hiveConfig = new HiveConfig(originalsStrings());
    ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef
            (PARTITIONER_CLASS_RECOMMENDER);
    partitionerConfig = new PartitionerConfig(partitionerConfigDef,
            originalsStrings());

    this.name = parseName(originalsStrings());
    addToGlobal(hiveConfig);
    addToGlobal(partitionerConfig);
    addToGlobal(commonConfig);
    addToGlobal(this);
  }

  private void addToGlobal(AbstractConfig config) {
    allConfigs.add(config);
    addConfig(config.values(), (ComposableConfig) config);
  }

  private void addConfig(Map<String, ?> parsedProps, ComposableConfig
          config) {
    for (String key : parsedProps.keySet()) {
      propertyToConfig.put(key, config);
    }
  }

  public String getAvroCodec() {
    return getString(AVRO_CODEC_CONFIG);
  }

  protected static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "azblob-sink";
  }

  public String getName() {
    return name;
  }

  @Override
  public Object get(String key) {
    ComposableConfig config = propertyToConfig.get(key);
    if (config == null) {
      throw new ConfigException(String.format("Unknown configuration " +
              "'%s'", key));
    }
    return config == this ? super.get(key) : config.get(key);
  }

  public Map<String, ?> plainValues() {
    Map<String, Object> map = new HashMap<>();
    for (AbstractConfig config : allConfigs) {
      map.putAll(config.values());
    }
    return map;
  }


  private static class PartRange implements ConfigDef.Validator {
    // AZ specific limit // TODO check this value
    final int min = 5 * 1024 * 1024;
    // Connector specific
    final int max = Integer.MAX_VALUE;

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException(name, value, "Part size must be " +
                "non-null");
      }
      Number number = (Number) value;
      if (number.longValue() < min) {
        throw new ConfigException(name, value,
                "Part size must be at least: " + min + " bytes (5MB)");
      }
      if (number.longValue() > max) {
        throw new ConfigException(name, value,
                "Part size must be no more: " + Integer.MAX_VALUE + "" +
                        " bytes (~2GB)");
      }
    }

    public String toString() {
      return "[" + min + ",...," + max + "]";
    }
  }

  public String getByteArrayExtension() {
    return getString(FORMAT_BYTEARRAY_EXTENSION_CONFIG);
  }

  public String getFormatByteArrayLineSeparator() {
    // White space is significant for line separators, but ConfigKey
    // trims it out,
    // so we need to check the originals rather than using the normal
    // machinery.
    if (originalsStrings().containsKey
            (FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG)) {
      return originalsStrings().get
              (FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG);
    }
    return FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT;
  }


  public static ConfigDef getConfig() {
    // Define the names of the configurations we're going to override
    Set<String> skip = new HashSet<>();
    skip.add(StorageSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);

    // Order added is important, so that group order is maintained
    ConfigDef visible = new ConfigDef();
    addAllConfigKeys(visible, newConfigDef(), skip);
    addAllConfigKeys(visible, StorageCommonConfig.newConfigDef
            (STORAGE_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, PartitionerConfig.newConfigDef
            (PARTITIONER_CLASS_RECOMMENDER), skip);

    return visible;
  }

  private static void addAllConfigKeys(ConfigDef container, ConfigDef
          other, Set<String> skip) {
    for (ConfigDef.ConfigKey key : other.configKeys().values()) {
      if (skip != null && !skip.contains(key.name)) {
        container.define(key);
      }
    }
  }

  public String getAdlDatalakeName() {
    return getString(ADL_DATALAKE_NAME);
  }

  public String getAdlClientId() {
    return getString(ADL_CLIENT_ID);
  }

  public String getAdlClientSecret() {
    return getString(ADL_CLIENT_SECRET);
  }

  public String getAdlTokenEndpoint() {
    return getString(ADL_TOKEN_ENDPOINT);
  }

  public static void main(String[] args) throws IOException {
    Properties properties = new Properties();
    properties.load(new FileInputStream(new File
            ("kafka-connect-adl/config/quickstart-adl.properties")));
    Map<String, String> map = new HashMap<>();
    for (final String name : properties.stringPropertyNames()) {
      map.put(name, properties.getProperty(name));
    }
    AdlSinkConnectorConfig adlSinkConnectorConfig = new
            AdlSinkConnectorConfig(map);
    System.out.println(adlSinkConnectorConfig.getString(ADL_DATALAKE_NAME));
    System.out.println(adlSinkConnectorConfig.getString
            (ADL_TOKEN_ENDPOINT));
    System.out.println(adlSinkConnectorConfig.getString(ADL_CLIENT_ID));
    System.out.println(adlSinkConnectorConfig.getString(ADL_CLIENT_SECRET));
    //System.out.println(getConfig().toEnrichedRst());
  }

}

