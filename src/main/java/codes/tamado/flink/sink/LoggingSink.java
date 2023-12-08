package codes.tamado.flink.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSink implements SinkFunction<String> {
  private static final long serialVersionUID = 0L;
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingSink.class);

  private final String prefix;

  public LoggingSink(final String prefix) {
    this.prefix = prefix;
  }

  @Override
  public void invoke(final String value, final Context context) throws Exception {
    LOGGER.info("{}: {}", this.prefix, value);
  }
}
