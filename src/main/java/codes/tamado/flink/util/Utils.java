package codes.tamado.flink.util;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  private static final String PROPERTIES = "properties";

  private Utils() {}

  /**
   * Parses arguments and assign into {@link ParameterTool} parameters.
   *
   * @param args Flink job arguments
   * @return the {@link ParameterTool} parameters
   */
  public static ParameterTool parseArgs(final String[] args) {
    final ParameterTool params = ParameterTool.fromArgs(args);

    if (params.has(PROPERTIES)) {
      try {
        final String localParams = params.getRequired(PROPERTIES);

        return ParameterTool.fromPropertiesFile(localParams).mergeWith(params);
      } catch (final IOException | RuntimeException e) {
        LOGGER.error("Cannot read properties file.", e);
      }
    }

    return params;
  }

  /**
   * Extracts prefixed Java {@link Properties} from specified {@link ParameterTool}.
   *
   * @param params Flink {@link ParameterTool}
   * @param prefix prefix for actual properties key
   * @return extracted properties without key prefix
   */
  public static Properties extractPrefixedProperties(
      final ParameterTool params,
      final String prefix) {
    final Properties paramsProperties = params.getProperties();
    final Properties properties = new Properties();

    paramsProperties.stringPropertyNames().stream()
        .filter(propKey -> propKey.startsWith(prefix))
        .forEach(propKey -> {
          final String key = propKey.substring(prefix.length());

          properties.setProperty(key, paramsProperties.getProperty(propKey));
        });

    return properties;
  }
}
