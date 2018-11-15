package jk.bigdata.tech.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Version {
  private static final Logger log = LoggerFactory.getLogger(Version.class);
  private static final String VERSION;

  static {
    String versionProperty = "unknown";
    try {
      Properties props = new Properties();
      props.load(Version.class.getResourceAsStream
              ("/kafka-connect-adl-version.properties"));
      versionProperty = props.getProperty("version", versionProperty)
              .trim();
    } catch (Exception e) {
      log.warn("Error while loading version:", e);
      versionProperty = "unknown";
    }
    VERSION = versionProperty;
  }

  public static String getVersion() {
    return VERSION;
  }
}
