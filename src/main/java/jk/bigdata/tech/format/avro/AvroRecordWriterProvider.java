package jk.bigdata.tech.format.avro;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import jk.bigdata.tech.AdlSinkConnectorConfig;
import jk.bigdata.tech.storage.AdlStorage;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AvroRecordWriterProvider implements
        RecordWriterProvider<AdlSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger
          (AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final AdlStorage storage;
  private final AvroData avroData;

  AvroRecordWriterProvider(AdlStorage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final AdlSinkConnectorConfig conf,
                                      final String filename) {
    // This is not meant to be a thread-safe writer!
    return new RecordWriter() {
      final DataFileWriter<Object> writer = new DataFileWriter<>(new
              GenericDatumWriter<>());
      Schema schema = null;
      ADLFileOutputStream adlOutputStream;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            adlOutputStream = storage.create(filename, true);
            org.apache.avro.Schema avroSchema = avroData
                    .fromConnectSchema(schema);
            writer.setCodec(CodecFactory.fromString(conf
                    .getAvroCodec()));
            writer.create(avroSchema, adlOutputStream);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record);
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          // AvroData wraps primitive types so their schema can be
          // included. We need to unwrap
          // NonRecordContainers to just their value to properly
          // handle these types
          if (value instanceof NonRecordContainer) {
            value = ((NonRecordContainer) value).getValue();
          }
          writer.append(value);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        log.debug("Committing");
        try {
          // Flush is required here, because closing the writer
          // will close the underlying AZ output
          // stream before committing any data to AZ.
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
  }
}
