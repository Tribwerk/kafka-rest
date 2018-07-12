/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.v2;

import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.RawConsumerRecord;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;

public class RawKafkaConsumerState extends KafkaConsumerState<String, String, String, String> {

  private static final Decoder<String> decoder = new StringDecoder(new VerifiableProperties());

  public RawKafkaConsumerState(KafkaRestConfig config,
                               ConsumerInstanceId instanceId,
                               Consumer consumer) {
    super(config, instanceId, consumer);
  }

  @Override
  protected Decoder<String> getKeyDecoder() {
    return decoder;
  }

  @Override
  protected Decoder<String> getValueDecoder() {
    return decoder;
  }

  @Override
  public ConsumerRecordAndSize<String, String> createConsumerRecord(
      ConsumerRecord<String, String> record) {
    long approxSize = 0;

    String key = null;
    String value = null;

    // The extra serialization here is unfortunate. We could use @JsonRawValue
    // and just use the raw bytes, but that risks returning invalid data to the user
    // if their data is not actually JSON encoded.

    if (record.key() != null) {
      approxSize += record.key().length();
      key = record.key();
    }

    if (record.value() != null) {
      approxSize += record.value().length();
      value = record.value();
    }

    return new ConsumerRecordAndSize<>(
        new RawConsumerRecord(record.topic(), key, value, record.partition(), record.offset()),
        approxSize);
  }

  private String deserialize(byte[] data) {
    try {
      return new String(data);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }
}
