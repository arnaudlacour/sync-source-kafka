
package com.pingidentity.sync.src;

import com.unboundid.directory.sdk.common.api.ServerThread;
import com.unboundid.directory.sdk.common.internal.Reconfigurable;
import com.unboundid.directory.sdk.common.types.*;
import com.unboundid.directory.sdk.sync.api.SyncSource;
import com.unboundid.directory.sdk.sync.config.SyncSourceConfig;
import com.unboundid.directory.sdk.sync.types.*;
import com.unboundid.ldap.sdk.*;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.util.args.*;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;
import com.unboundid.util.ssl.TrustStoreTrustManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.currentTimeMillis;

public class Kafka extends SyncSource implements Reconfigurable<SyncSourceConfig> {
  private final static String TOPIC = "kafka.topic";
  private final static String PROPS = "kafka.properties";
  private final static String TIMEOUT = "kafka.poll.timeout";
  private final static String STATEINTERVAL = "kafka.state.save.ms";
  private final static String FILTERKEY = "kafka.filter.key";
  private final static String FILTERVALUE = "kafka.filter.value";


  private final static String SERVER = "ldap.server.address";
  private final static String PORT = "ldap.server.port";
  private final static String DN = "ldap.user.dn";
  private final static String PWD = "ldap.user.password";
  private final static String PWDFILE = "ldap.user.password.file";
  private final static String USESSL = "ldap.use.ssl";
  private final static String TRUSTSTOREPATH = "ldap.truststore.path";
  private final static String TRUSTSTOREPIN = "ldap.truststore.pin";
  private final static String TRUSTSTOREPINFILE = "ldap.truststore.pinfile";
  private final static String MAXCONNS = "ldap.pool.conn.max";
  private final static String INITCONNS = "ldap.pool.conn.init";


  private SyncServerContext sc;
  private LDAPConnectionPool pool;
  private String kafkaTopic;
  private Properties kafkaProps;
  private String host;
  private int port;
  private String user;
  private String pwd;
  private Queue<ChangeRecord> changeQueue = new ConcurrentLinkedQueue<>();
  private ExecutorService executor;
  private SyncSourceConfig cfg;
  private KafkaPoller kafkaPoller;
  private boolean useSSL;
  private File trustStoreFile;
  private String trustStorePin = null;
  // private FileArgument trustStoreFileArg;
  private int connMax;
  private int connInit;
  private int timeout;
  //private KafkaState kafkaState = new KafkaState(END);
  private Map<Integer, Long> kafkaState = null;
  private Long stateInterval;
  private Thread kafkaPollerThread;

  @Override
  public String getExtensionName() {
    return "Apache Kafka Sync Source";
  }

  @Override
  public String[] getExtensionDescription() {
    return new String[]{"This Sync Source is built to consume data from Apache Kafka 09 and later and use the information in Kafka08 messages to check updated entries in an LDAP server"};
  }

  @Override
  public String getCurrentEndpointURL() {
    if (kafkaProps == null) return "kafka://null";
    return "kafka://" + kafkaProps.get("bootstrap.servers") + "/" + kafkaTopic + "?group=" + kafkaProps.get("group.id");
  }

  @Override
  public void defineConfigArguments(final ArgumentParser parser)
      throws ArgumentException {
    parser.addArgument(new FileArgument(null, PROPS, false, 1, "{properties.file}", "Kafka configuration properties file"));
    parser.addArgument(new StringArgument(null, TOPIC, false, 1, "{topicName}", "Kafka topic to subscribe to. Specify one or more topics.(default:ping-data-sync-topic)", "ping-data-sync-topic"));
    parser.addArgument(new IntegerArgument(null, TIMEOUT, false, 1, "{ms}", "Kafka polling timeout", 100));
    parser.addArgument(new IntegerArgument(null, STATEINTERVAL, false, 1, "{ms}", "Interval at which the state is saved", 1000));

    StringArgument filterKeyArg = new StringArgument(null, FILTERKEY, false, 1, "{key}", "A JSON key to filter kafka events on. For example, if " +
        " you want to filter objects that have {'myKey': 'theValue'}, use myKey here");
    parser.addArgument(filterKeyArg);

    StringArgument filterValueArg = new StringArgument(null, FILTERVALUE, false, 0, "{value}", "One or more values that the provided JSON must have to be selected as candidate Kafka events. In the previous example, the value to provide this argument would be theValue.");
    parser.addArgument(filterValueArg);

    // if a key is provided, so must a value to use as filter
    parser.addDependentArgumentSet(filterKeyArg,filterValueArg);

    parser.addArgument(new StringArgument(null, SERVER, true, 1, "{host}", "Directory Server hostname or IP address. (default:localhost)", "localhost"));
    parser.addArgument(new IntegerArgument(null, PORT, true, 1, "{port}", "Directory Server port number (default:389)", 389));
    parser.addArgument(new StringArgument(null, DN, true, 1, "{user}", "DN used to bind to the directory server. The user must be able to read any entry. (default: cn=directory manager)", "cn=directory manager"));

    StringArgument pwdArg = new StringArgument(null, PWD, false, 1, "{password}", "Password used to bind to the directory server");
    parser.addArgument(pwdArg);

    FileArgument pwdFileArg = new FileArgument(null, PWDFILE, false, 1, "{file}", "Path to a file containing the password");
    parser.addArgument(pwdFileArg);

    parser.addExclusiveArgumentSet(pwdArg, pwdFileArg);


    BooleanArgument useSSLArg = new BooleanArgument(null, USESSL, "Use SSL for secure communication with the directory server");
    parser.addArgument(useSSLArg);

    FileArgument truststoreFileArg = new FileArgument(null, TRUSTSTOREPATH, false, 1, "{truststore}", "Certificate truststore path");
    parser.addArgument(truststoreFileArg);

    FileArgument truststorePinFileArg = new FileArgument(null, TRUSTSTOREPINFILE, false, 1, "{truststore}", "Certificate truststore PIN file");
    StringArgument truststorePinArg = new StringArgument(null, TRUSTSTOREPIN, false, 1, "{pin}", "Certificate truststore PIN");
    parser.addArgument(truststorePinFileArg);
    parser.addArgument(truststorePinArg);
    parser.addExclusiveArgumentSet(truststorePinArg, truststorePinFileArg);

    parser.addArgument(new IntegerArgument(null, INITCONNS, false, 1, "{num-conn}", "Initial number of connections in the connection pool (default:4)", 1, 100000, 4));
    parser.addArgument(new IntegerArgument(null, MAXCONNS, false, 1, "{num-conn}", "Maximum number of connections in the connection pool (default:10)", 1, 100000, 10));
  }

  private void log(LogSeverity severity, final String message) {
    if (sc != null) {
      if (severity == null) {
        severity = LogSeverity.INFO;
      }
      sc.logMessage(severity, message != null ? message : "no message provided");
    } else {
      System.out.println(this.getClass().getCanonicalName() + " - " + message);
    }
  }

  private void logInfo(final String message) {
    log(LogSeverity.INFO, message);
  }

  private void logError(final String message) {
    log(LogSeverity.SEVERE_ERROR, message);
  }

  private void logDebug(final String message) {
    log(LogSeverity.DEBUG, message);
  }

  private void debug(final Throwable t) {
    if (sc != null) {
      logDebug(t.getMessage());
      sc.debugCaught(t);
    } else {
      System.out.println(t.getMessage());
    }
  }

  private void initializeLDAPConnectionPool() throws LDAPException, GeneralSecurityException {
    try {
      LDAPConnection ldapx;
      if (useSSL) {
        SSLUtil sslUtil;
        if (trustStoreFile != null) {
          sslUtil = new SSLUtil(new TrustStoreTrustManager(trustStoreFile, trustStorePin.toCharArray(), null, true));
        } else {
          sslUtil = new SSLUtil(new TrustAllTrustManager());
        }
        ldapx = new LDAPConnection(sslUtil.createSSLSocketFactory());
        ldapx.connect(host, port);
        ldapx.bind(user, pwd);
      } else {
        //TODO: add StartTLS support
        ldapx = new LDAPConnection(host, port, user, pwd);
      }
      pool = new LDAPConnectionPool(ldapx, connInit, connMax);
      logInfo("Connection pool to ldap" + (useSSL ? "s" : "") + "://" + host + ":" + port + " established as " + user);
    } catch (LDAPException ldape) {
      pool = null;
      throw ldape;
    } catch (GeneralSecurityException gse) {
      pool = null;
      throw gse;
    }
  }

  private void initializeKafkaClient() {
    executor = Executors.newFixedThreadPool(1);
    kafkaPoller = new KafkaPoller(kafkaProps, kafkaTopic);
    kafkaPollerThread = sc.createThread(kafkaPoller, "Kafka Poller for " + cfg.getConfigObjectName());
    executor.submit(kafkaPollerThread);
    logInfo("All threads created.");
  }

  private void parseProvidedArguments(final ArgumentParser parser, boolean dryRun) throws Exception {
    Properties localKafkaProps = new Properties();
    String localKafkaTopic;
    String localHost;
    Integer localPort;
    String localUser;
    String localPwd;
    boolean localUseSSL;
    FileArgument localTrustStoreFileArg = null;
    File localTrustStoreFile = null;
    int localConnInit;
    int localConnMax;
    String localTrustStorePin = null;

    FileArgument kafkaPropsArg = (FileArgument) parser.getNamedArgument(PROPS);
    if (kafkaPropsArg.isPresent()) {
      localKafkaProps.load(new FileInputStream(kafkaPropsArg.getValue()));
    }
    if (!localKafkaProps.containsKey("bootstrap.servers")) {
      logInfo("No bootstrap servers provided. Loading default.");
      localKafkaProps.put("bootstrap.servers", "localhost:9092");
    }
    if (!localKafkaProps.containsKey("group.id")) {
      logInfo("No group ID provided. Loading default.");
      localKafkaProps.put("group.id", "ping-data-sync-group");
    }
    if (!localKafkaProps.containsKey("key.deserializer")) {
      logInfo("No key deserializer provided. Loading default.");
      localKafkaProps.put("key.deserializer", StringDeserializer.class.getName());
    }
    if (!localKafkaProps.containsKey("value.deserializer")) {
      logInfo("No value deserializer provided. Loading default.");
      localKafkaProps.put("value.deserializer", StringDeserializer.class.getName());
    }
    localKafkaTopic = ((StringArgument) parser.getNamedArgument(TOPIC)).getValue();

    localHost = ((StringArgument) parser.getNamedArgument(SERVER)).getValue();
    localPort = ((IntegerArgument) parser.getNamedArgument(PORT)).getValue();
    localUser = ((StringArgument) parser.getNamedArgument(DN)).getValue();

    Integer localInterval = ((IntegerArgument) parser.getNamedArgument(STATEINTERVAL)).getValue();

    StringArgument localPwdArg = (StringArgument) parser.getNamedArgument(PWD);
    FileArgument localPwdFileArg = (FileArgument) parser.getNamedArgument(PWDFILE);
    if (localPwdFileArg.isPresent()) {
      localPwd = new String(Files.readAllBytes(localPwdFileArg.getValue().toPath()));
    } else {
      localPwd = localPwdArg.getValue();
    }
    if (localPwd == null)
      throw new Exception("No password was provided to establish LDAP connections to the server");

    int localTimeout = ((IntegerArgument) parser.getNamedArgument(TIMEOUT)).getValue();

    localUseSSL = parser.getNamedArgument(USESSL).isPresent();
    if (localUseSSL) {
      localTrustStoreFileArg = (FileArgument) parser.getNamedArgument(TRUSTSTOREPATH);
      if (localTrustStoreFileArg.isPresent()) {
        localTrustStoreFile = localTrustStoreFileArg.getValue();
        FileArgument localTrustStorePinFileArg = (FileArgument) parser.getNamedArgument(TRUSTSTOREPINFILE);
        if (localTrustStorePinFileArg.isPresent()) {
          File localTrustStorePinFile = localTrustStorePinFileArg.getValue();
          localTrustStorePin = new String(Files.readAllBytes(localTrustStorePinFile.toPath()));
        } else localTrustStorePin = ((StringArgument) parser.getNamedArgument(TRUSTSTOREPIN)).getValue();
      }
    }
    localConnInit = ((IntegerArgument) parser.getNamedArgument(INITCONNS)).getValue();
    localConnMax = ((IntegerArgument) parser.getNamedArgument(MAXCONNS)).getValue();
    if (localConnInit > localConnMax) {
      throw new Exception("The maximum number of connection in the pool must be greater or equal to the number of initial connections");
    }

    if (!dryRun) {
      // Kafka
      kafkaProps = localKafkaProps;
      kafkaTopic = localKafkaTopic;

      //LDAP
      host = localHost;
      port = localPort;
      user = localUser;
      pwd = localPwd;
      useSSL = localUseSSL;
      if (useSSL) {
        trustStoreFile = localTrustStoreFile;
        trustStorePin = localTrustStorePin;
      }
      connMax = localConnMax;
      connInit = localConnInit;
      timeout = localTimeout;
      stateInterval = new Long(localInterval);
    }
  }

  @Override
  public void initializeSyncSource(final SyncServerContext serverContext,
                                   final SyncSourceConfig config, final ArgumentParser parser) {
    logInfo("Kafka Sync source initialization starting for " + config.getConfigObjectName());
    cfg = config;
    sc = serverContext;
    try {
      parseProvidedArguments(parser, false);
      initializeLDAPConnectionPool();
      initializeKafkaClient();
    } catch (IOException ioe) {
      logError(ioe.getMessage());
      debug(ioe);
    } catch (LDAPException le) {
      logError(le.getMessage());
      debug(le);
    } catch (GeneralSecurityException gse) {
      logError(gse.getMessage());
      debug(gse);
    } catch (ConfigException ce) {
      logError(ce.getMessage());
      debug(ce);
    } catch (Exception e) {
      logError(e.getMessage());
      debug(e);
    }
    logInfo("Kafka Sync source initialization done for " + cfg.getConfigObjectName());
  }

  @Override
  public void finalizeSyncSource() {
    logInfo("Initiating Kafka Sync source " + cfg.getConfigObjectName() + " shutdown ...");
    if (kafkaPoller != null) kafkaPoller.shutdown();
    if (executor != null) executor.shutdown();
    if (pool != null) pool.close();
    logInfo("Kafka Sync source shutdown complete for " + cfg.getConfigObjectName());
  }

  @Override
  public Serializable getStartpoint() {
    return (Serializable) kafkaState;
    //return 0L;
  }

  @Override
  public void setStartpoint(SetStartpointOptions options) throws EndpointException {
    log(LogSeverity.DEBUG, "Kafka Sync source start point processing...");
    KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
    kafkaConsumer.subscribe(Arrays.asList(kafkaTopic));
    switch (options.getStartpointType()) {
      case BEGINNING_OF_CHANGELOG:
        logDebug("Attempting to resume operation at the beginning of the Kafka topic");
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
        /*
        Removed because https://jira.unboundid.lab/browse/DS-17347
        logDebug("Attempting to resume operation a the beginning of the kafka topic");
        kafkaState = new KafkaState(BEGINNING);
        */
        break;
      case END_OF_CHANGELOG:
        logDebug("Attempting to resume operation a the end of the kafka topic");
        kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
        /*
        Removed because https://jira.unboundid.lab/browse/DS-17347
        kafkaState = new KafkaState(END);
        */
        break;
      case RESUME_AT_SERIALIZABLE:
        logDebug("Attempting to resume operation for sync pipe with seriablizable");
        Object localKafkaState = options.getSerializableValue();
        if (localKafkaState != null && localKafkaState instanceof Map) {
          logDebug("Restoring kafka state.");
          kafkaState = (Map<Integer, Long>) localKafkaState;
          for (TopicPartition topicPartition : kafkaConsumer.assignment()) {
            if (kafkaState.containsKey(topicPartition.partition())){
              logDebug("Seeking to position "+kafkaState.get(topicPartition.partition())+" for patition "+topicPartition.partition()+" in topic "+kafkaTopic);
              kafkaConsumer.seek(topicPartition,kafkaState.get(topicPartition.partition()));
            } else {
              logDebug("Seeking to the end of partition "+topicPartition.partition()+" in topic "+kafkaTopic);
              kafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
            }
          }

          /*
          Set<TopicPartition> tps = new HashSet<>();
          for( Integer partition: kafkaState.keySet()){
            logDebug("Assigning partition "+partition);
            tps.add(new TopicPartition(kafkaTopic,partition));
          }
          kafkaConsumer.assign(tps);
          for(Integer partition: kafkaState.keySet()){
            logDebug("Seeking to position "+kafkaState.get(partition)+ " in partition "+partition);
            kafkaConsumer.seek(new TopicPartition(kafkaTopic,partition),kafkaState.get(partition));
          }
          */
        } else {
          logDebug("Could not restore Kafka state. Setting startpoint to the end of kafka topic " + kafkaTopic);
          kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
          /*
          Removed because https://jira.unboundid.lab/browse/DS-17347
          kafkaState = new KafkaState(END);
          */
        }

        break;
      default:
        throw new IllegalArgumentException(
            "This start point type is not supported: "
                + options.getStartpointType().toString());
    }
    kafkaState = generateState(kafkaConsumer);
    kafkaConsumer.close();
    logDebug("Kafka Sync source start point processing done.");
  }

  @Override
  public List<ChangeRecord> getNextBatchOfChanges(int maxChanges, AtomicLong numStillPending) throws EndpointException {
    List<ChangeRecord> result = new ArrayList<>();

    int changesProcessed = 0;
    while (++changesProcessed < maxChanges && changeQueue.size() > 0) {
      ChangeRecord record = changeQueue.poll();
      if (record != null) result.add(record);
    }

    if (result.size() > 0)
      logDebug("Returning batch of " + result.size() + " change records");
    return result;
  }

  private ChangeRecord changeRecordBuilder(final String kafkaMessage) {
    ChangeRecord result = null;
    if (kafkaMessage == null) return result;

    JSONParser p = new JSONParser();
    JSONObject kafkaEvent = null;
    try {
      kafkaEvent = (JSONObject) p.parse(kafkaMessage);
    } catch (ParseException e) {
      //debug(e);
      logError("Received malformed JSON message. Ignoring.");
      if (e != null && e.getMessage() != null && !e.getMessage().isEmpty()) logDebug(e.getMessage());
      return result;
    }
    if (kafkaEvent == null) return result;

    logInfo("KafkaSyncSource received event " + kafkaEvent.toString());

    DN dn;
    try {
      dn = new DN((String) kafkaEvent.get("dn"));
      logDebug("KafkaSyncSource received event DN " + dn);
    } catch (LDAPException e) {
      debug(e);
      return result;
    }
    if (dn == null) return result;

    String type = (String) kafkaEvent.get("type");
    logInfo("KafkaSyncSource received event type " + type);
    ChangeType changeType = ChangeType.MODIFY;
    if ("ADD".equalsIgnoreCase(type)) {
      changeType = ChangeType.ADD;
    } else if ("REM".equalsIgnoreCase(type)) {
      changeType = ChangeType.DELETE;
    } else if ("MODDN".equalsIgnoreCase(type)) {
      changeType = ChangeType.MODIFY_DN;
    }

    ChangeRecord.Builder builder = new ChangeRecord.Builder(changeType, dn);

    JSONArray attrs = (JSONArray) kafkaEvent.get("attrs");
    if (attrs != null) {
      String[] changedAttributes = new String[attrs.size()];
      Iterator<String> i = attrs.iterator();
      int j = 0;
      while (i.hasNext()) changedAttributes[j++] = i.next();
      builder.changedAttributes(changedAttributes);
      logDebug("Added list of changed attributes " + attrs.toJSONString());
    }

    String modifier = (String) kafkaEvent.get("bind");
    if (modifier != null) {
      builder.modifier(modifier);
      logDebug("Added modifier [" + modifier + "]");
    }

    Long time = (Long) kafkaEvent.get("time");
    if (time != null) {
      builder.changeTime(time);
      logDebug("Added change time [" + time + "]");
    }

    result = builder.build();
    logDebug("Change record added to return list");

    return result;
  }

  @Override
  public Entry fetchEntry(SyncOperation operation) throws EndpointException {
    if (pool == null) try {
      initializeLDAPConnectionPool();
    } catch (LDAPException le) {
      throw new EndpointException(PostStepResult.RETRY_OPERATION_LIMITED, le.getMessage());
    } catch (GeneralSecurityException gse) {
      throw new EndpointException(PostStepResult.RETRY_OPERATION_LIMITED, gse.getMessage());
    }

    if (pool == null)
      throw new EndpointException(PostStepResult.ABORT_OPERATION, "LDAP connection pool could not be initialized");
    if (operation == null)
      return null;
    if (operation.getIdentifiableInfo() == null)
      return null;
    try {
      logInfo("Attempting retrieval for entry " + operation.getIdentifiableInfo());
      return pool.getEntry(operation.getIdentifiableInfo());
    } catch (LDAPException e) {
      throw new EndpointException(e);
    }
  }

  @Override
  public void acknowledgeCompletedOps(LinkedList<SyncOperation> completedOps) throws EndpointException {
  }

  @Override
  public boolean isConfigurationAcceptable(SyncSourceConfig syncSourceConfig, ArgumentParser argumentParser, List<String> list) {
    try {
      parseProvidedArguments(argumentParser, false);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  @Override
  public ResultCode applyConfiguration(SyncSourceConfig syncSourceConfig, ArgumentParser argumentParser, List<String> list, List<String> list1) {
    try {
      parseProvidedArguments(argumentParser, true);
      initializeLDAPConnectionPool();
      initializeKafkaClient();
      return ResultCode.SUCCESS;
    } catch (IOException ioe) {
      return ResultCode.OTHER;
    } catch (LDAPException le) {
      return le.getResultCode();
    } catch (GeneralSecurityException gse) {
      return ResultCode.LOCAL_ERROR;
    } catch (Exception e) {
      return ResultCode.CANCELED;
    }
  }


  Map<Integer, Long> generateState(KafkaConsumer<String,String> kc) {
    Map<Integer, Long> newState = new ConcurrentHashMap<>();
    if (kc != null) {
      for (TopicPartition topicPartition : kc.assignment()) {
        logDebug("Converting state for partition "+topicPartition.partition());
        newState.put(topicPartition.partition(), kc.position(topicPartition));
      }
    }
    return newState;
  }

  private class KafkaPoller implements ServerThread {
    AtomicBoolean keepRunning = new AtomicBoolean(true);
    KafkaConsumer<String, String> threadLocalConsumer;
    AtomicLong lastCommitTime = new AtomicLong(0L);

    public KafkaPoller(Properties props, String topic) {
      threadLocalConsumer = new KafkaConsumer(props);
      logInfo("Subscribing to topic [" + kafkaTopic + "]");
      threadLocalConsumer.subscribe(Arrays.asList(topic));
    }

    public void runThread() {
      try {
        while (keepRunning.get()) {
          ConsumerRecords<String, String> kafkaRecords = threadLocalConsumer.poll(timeout);
          for (ConsumerRecord<String, String> kafkaRecord : kafkaRecords) {
            if (kafkaRecord == null ) continue;

            logInfo("Received record #" + kafkaRecord.offset() + " in partition " + kafkaRecord.partition() + " of topic " + kafkaRecord.topic() + " (" + kafkaRecord.key() + ")");
            if ( kafkaRecord.value() == null) continue;

            final ChangeRecord changeRecord = changeRecordBuilder(kafkaRecord.value());
            if (changeRecord == null) continue;

            changeQueue.add(changeRecord);
            long now = currentTimeMillis();
            if ((now - lastCommitTime.get()) > stateInterval) {
              kafkaState= generateState(threadLocalConsumer);
              lastCommitTime.set(now);
            }
          }
        }
      } catch (WakeupException e) {              // Ignore exception if closing
        if (keepRunning.get()) throw e;
      } finally {
        kafkaState=generateState(threadLocalConsumer);
        threadLocalConsumer.close();
      }
    }

    public void shutdown() {
      logInfo("Shutting down kafka poller");
      kafkaState = generateState(threadLocalConsumer);
      keepRunning.set(false);
      threadLocalConsumer.wakeup();
      logInfo("Kafka poller shutdown sequence complete");
    }
  }
}