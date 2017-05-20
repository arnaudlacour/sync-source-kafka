package com.pingidentity.sync.src;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.pingidentity.sync.src.KafkaStartpoint.END;
import static com.pingidentity.sync.src.KafkaStartpoint.RESUME;

/**
 * Created by arnaudlacour on 1/21/17.
 */
public class KafkaState implements Serializable {
  private KafkaStartpoint type = RESUME;
  private Map<TopicPartition, Long> state;

  public KafkaState(KafkaStartpoint startpoint, Map<TopicPartition, Long> map) {
    type = startpoint;
    state = map;
  }

  public KafkaState(KafkaStartpoint startpoint) {
    this(startpoint, new ConcurrentHashMap<TopicPartition, Long>());
  }

  public KafkaState() {
    this(END);
  }

  public KafkaStartpoint getStartpoint() {
    return type;
  }

  public KafkaState setStartpoint(KafkaStartpoint startpoint) {
    type = startpoint;
    return this;
  }

  public Map<TopicPartition, Long> getState() {
    return state;
  }

  public KafkaState setState(Map<TopicPartition, Long> map) {
    state = map;
    type = RESUME;
    return this;
  }

  public Long getPosition(TopicPartition topicPartition) {
    return state.get(topicPartition);
  }
}
