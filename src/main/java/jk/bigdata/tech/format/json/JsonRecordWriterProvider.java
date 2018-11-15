package jk.bigdata.tech.format.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import jk.bigdata.tech.AdlSinkConnectorConfig;
import jk.bigdata.tech.storage.AdlStorage;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonRecordWriterProvider implements
        RecordWriterProvider<AdlSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger
          (JsonRecordWriterProvider.class);
  private static final String EXTENSION = ".json";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR
          .getBytes();
  private final AdlStorage storage;
  private final ObjectMapper mapper;
  private final JsonConverter converter;

  JsonRecordWriterProvider(AdlStorage storage, JsonConverter converter) {
    this.storage = storage;
    this.mapper = new ObjectMapper();
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final AdlSinkConnectorConfig conf,
                                      final String filename) {
    try {
      return new RecordWriter() {
        final ADLFileOutputStream adlFileOutputStream = storage
                .create(filename, true);
        final JsonGenerator writer = mapper.getFactory()
                .createGenerator(adlFileOutputStream)
                .setRootValueSeparator(null);

        @Override
        public void write(SinkRecord record) {
          log.trace("Sink record: {}", record);
          try {
            Object value = record.value();
            if (value instanceof Struct) {
              byte[] rawJson = converter
                      .fromConnectData(record.topic(), record
                              .valueSchema(), value);
              adlFileOutputStream.write(rawJson);
              adlFileOutputStream.write(LINE_SEPARATOR_BYTES);
            } else {
              writer.writeObject(value);
              writer.writeRaw(LINE_SEPARATOR);
            }
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public void commit() {
          log.debug("Committing");
          try {
            // Flush is required here, because closing the writer
            // will close the underlying AZ
            // output stream before committing any data to AZ.
            writer.flush();
            writer.close();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public void close() {
          log.debug("Closing writer");
          try {
            writer.close();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
      };
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}
