package jk.bigdata.tech.storage;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;
import jk.bigdata.tech.AdlSinkConnectorConfig;
import org.apache.avro.file.SeekableInput;

import java.io.IOException;
import java.io.OutputStream;

import static io.confluent.connect.storage.common.util.StringUtils.isNotBlank;

public class AdlStorage implements Storage<AdlSinkConnectorConfig,
        Iterable<DirectoryEntry>> {

  private final AdlSinkConnectorConfig conf;
  private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 " +
          "KafkaADLConnector/%s";
  // Create client object using client creds
  private ADLStoreClient adlStoreClient;


  public AdlStorage(AdlSinkConnectorConfig conf, String url) {
    this.conf = conf;
    AccessTokenProvider provider = new ClientCredsTokenProvider(conf
            .getAdlTokenEndpoint(), conf.getAdlClientId(), conf
            .getAdlClientSecret());
    adlStoreClient = ADLStoreClient.createClient(conf.getAdlDatalakeName
            (), provider);
  }

  // Visible for testing.
  public AdlStorage(AdlSinkConnectorConfig conf, String containerName,
                    ADLStoreClient adlStoreClient) {
    this.conf = conf;
    this.adlStoreClient = adlStoreClient;
  }

  @Override
  public boolean exists(String name) {
    try {
      return isNotBlank(name);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
  }

  @Override
  public String url() {
    //return container.getUri().toString();
    return null;
  }

  @Override
  public Iterable<DirectoryEntry> list(String path) {
    try {
      return adlStoreClient.enumerateDirectory(path);
    } catch (IOException e) {
      throw new IllegalArgumentException("Path does not exists");
    }
  }

  @Override
  public AdlSinkConnectorConfig conf() {
    return conf;
  }

  @Override
  public SeekableInput open(String path, AdlSinkConnectorConfig conf) {
    throw new UnsupportedOperationException(
            "File reading is not currently supported in AZ Data Lake " +
                    "Store");
  }

  @Override
  public boolean create(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream create(String path, AdlSinkConnectorConfig conf,
                             boolean overwrite) {
    return create(path, overwrite);
  }

  public ADLFileOutputStream create(String path, boolean overwrite) {
    if (!overwrite) {
      throw new UnsupportedOperationException(
              "Creating a file without overwriting is not currently " +
                      "supported in AZ Blob Connector");
    }

    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException("Path can not be empty!");
    }
    ADLFileOutputStream outputStream = null;
    try {
      outputStream = adlStoreClient.createFile(path, IfExists.OVERWRITE);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return outputStream;
  }

}
