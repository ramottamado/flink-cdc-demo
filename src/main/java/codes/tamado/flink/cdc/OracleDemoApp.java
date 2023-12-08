package codes.tamado.flink.cdc;

import java.util.Properties;

import codes.tamado.flink.sink.LoggingSink;
import codes.tamado.flink.util.Utils;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class OracleDemoApp {
  private static final String DEBEZIUM_PREFIX = "debezium.";

  public static void main(String[] args) throws Exception {
    ParameterTool params = Utils.parseArgs(args);
    Properties debeziumProperties = Utils.extractPrefixedProperties(params, DEBEZIUM_PREFIX);

    SourceFunction<String> sourceFunction = OracleSource.<String>builder()
        .hostname(params.getRequired("hostname"))
        .port(params.getInt("port", 1521))
        .database(params.getRequired("database"))
        .schemaList(params.getRequired("schema"))
        .tableList(params.getRequired("tables"))
        .username(params.getRequired("username"))
        .password(params.getRequired("password"))
        .startupOptions(StartupOptions.latest())
        .debeziumProperties(debeziumProperties)
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();

    try (StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
            new Configuration())) {

      // enable checkpoint
      // env.enableCheckpointing(3000);

      env.addSource(sourceFunction)
          .name("Oracle Source")
          .uid("oracle-source")
          .setParallelism(1) // set 1 parallel source tasks
          .addSink(new LoggingSink("Oracle"))
          .name("Logging Sink")
          .uid("logging-sink")
          .setParallelism(1); // use parallelism 1 for sink

      env.execute("Log Oracle CDC");
    }
  }
}