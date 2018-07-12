/**
 * Copyright 2015 Confluent Inc.
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
package io.confluent.kafkarest.integration;

import avro.shaded.com.google.common.collect.ImmutableCollection;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.*;
import junit.runner.Version;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.tools.ant.taskdefs.Basename;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertNoContentResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.*;


public class AbstractConsumerTest extends ClusterTestHarness {

  public AbstractConsumerTest() {
  }

  public AbstractConsumerTest(int numBrokers, boolean withSchemaRegistry) {
    super(numBrokers, withSchemaRegistry);
  }
  protected void produceBinaryMessages(List<ProducerRecord<byte[], byte[]>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    Producer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);
    for (ProducerRecord<byte[], byte[]> rec : records) {
      try {
        producer.send(rec).get();
      } catch (Exception e) {
        fail("Consumer test couldn't produce input messages to Kafka");
      }
    }
    producer.close();
  }

  protected void produceJsonMessages(List<ProducerRecord<Object, Object>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    Producer<Object, Object> producer = new KafkaProducer<Object, Object>(props);
    for (ProducerRecord<Object, Object> rec : records) {
      try {
        producer.send(rec).get();
      } catch (Exception e) {
        fail("Consumer test couldn't produce input messages to Kafka");
      }
    }
    producer.close();
  }


  protected void consumeJsonMessages(String topic, long count) {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-groupid");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    Consumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props);
    consumer.subscribe(Arrays.asList(topic));
    ConsumerRecords<Object, Object> consumerRecords = consumer.poll(4_000);
    assertEquals(consumerRecords.count(),count );
    consumer.close();
  }

  protected void produceRawMessages(List<ProducerRecord<Object, Object>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    Producer<String, String> producer = new KafkaProducer<>(props);
    for (ProducerRecord<Object, Object> rec : records) {
      ProducerRecord<String, String> stringRec = new ProducerRecord<>(rec.topic(), rec.key()==null?null:rec.key().toString(),  rec.value()==null?null:rec.value().toString());
      try {
        producer.send(stringRec).get();
      } catch (Exception e) {

        fail("Consumer test couldn't produce input messages to Kafka"+ e.getMessage());
      }
    }
    producer.close();
  }

  protected void produceAvroMessages(List<ProducerRecord<Object, Object>> records) {
    HashMap<String, Object> serProps = new HashMap<String, Object>();
    serProps.put("schema.registry.url", schemaRegConnect);
    final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(serProps, true);
    final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(serProps, false);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, avroKeySerializer, avroValueSerializer);
    for (ProducerRecord<Object, Object> rec : records) {
      try {
        producer.send(rec).get();
      } catch (Exception e) {
        fail("Consumer test couldn't produce input messages to Kafka");
      }
    }
    producer.close();
  }

  protected Response createConsumerInstance(String groupName, String id,
                                              String name, EmbeddedFormat format) {
    return createConsumerInstanceV2(groupName, id, name, format, Versions.KAFKA_MOST_SPECIFIC_DEFAULT, null );
  }

  protected Response createConsumerInstanceV2(String groupName, String id,
                                            String name, EmbeddedFormat format, String version, String autoOffsetReset) {
    ConsumerInstanceConfig config = null;
    if (id != null || name != null || format != null) {
      config = new ConsumerInstanceConfig(
          id, name, (format != null ? format.toString() : null), null, null);
    }
    return request("/consumers/" + groupName)
        .post(Entity.entity(config, version));
  }


  protected Response subscribeConsumerInstanceV2(String groupName, String topic,
                                                 String name){
    ConsumerSubscriptionRecord subscriptionRecord = new ConsumerSubscriptionRecord(Arrays.asList(topic), "");
    return request("/consumers/" + groupName + "/instances/"+name+"/subscription").post(Entity.entity(subscriptionRecord, Versions.KAFKA_V2_JSON));
  }

  protected String consumerNameFromInstanceUrl(String url) {
    try {
      String[] pathComponents = new URL(url).getPath().split("/");
      return pathComponents[pathComponents.length-1];
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  // Need to start consuming before producing since consumer is instantiated internally and
  // starts at latest offset
  protected String startConsumeMessages(String groupName, String topic, EmbeddedFormat format,
                                        String expectedMediatype) {
    return startConsumeMessages(groupName, topic, format, expectedMediatype, false);
  }

  protected void startConsumeMessagesV2(String groupName, String topic, EmbeddedFormat format,
                                        String expectedMediatype, String instanceName) {
    startConsumeMessagesV2(groupName, topic, format, expectedMediatype, instanceName, false);
  }

  protected void startConsumeMessagesV2(String groupName, String topic, EmbeddedFormat format,
                                        String expectedMediatype, String instanceName,
                                        boolean expectFailure) {

    Response createResponse = createConsumerInstanceV2(groupName, null, instanceName, format, Versions.KAFKA_V2_JSON, "smallest");
    assertOKResponse(createResponse, Versions.KAFKA_V2_JSON);
    assertTrue("Consumer creation response contains instance id "+instanceName, createResponse.readEntity(String.class).contains(instanceName));
    Response subscriptionResponse = subscribeConsumerInstanceV2(groupName, topic, instanceName);
    assertNoContentResponse(subscriptionResponse);
    Response subscriptionsResponse = request("/consumers/"+groupName+"/instances/"+instanceName+"/subscription").accept(Versions.KAFKA_V2_JSON).get();
    assertEquals("{\"topics\":[\""+topic+"\"]}", subscriptionsResponse.readEntity(String.class));
  }

  /**
   * Start a new consumer instance and start consuming messages. This expects that you have not
   * produced any data so the initial read request will timeout.
   *
   * @param groupName         consumer group name
   * @param topic             topic to consume
   * @param format            embedded format to use. If null, an null ConsumerInstanceConfig is
   *                          sent, resulting in default settings
   * @param expectedMediatype expected Content-Type of response
   * @param expectFailure     if true, expect the initial read request to generate a 404
   * @return the new consumer instance's base URI
   */
  protected String startConsumeMessages(String groupName, String topic, EmbeddedFormat format,
                                        String expectedMediatype,
                                        boolean expectFailure) {
    Response createResponse = createConsumerInstance(groupName, null, null, format);
    assertOKResponse(createResponse, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);

    CreateConsumerInstanceResponse instanceResponse =
            TestUtils.tryReadEntityOrLog(createResponse, CreateConsumerInstanceResponse.class);
    assertNotNull(instanceResponse.getInstanceId());
    assertTrue(instanceResponse.getInstanceId().length() > 0);
    assertTrue("Base URI should contain the consumer instance ID",
               instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()));

    // Start consuming. Since production hasn't started yet, this is expected to timeout.
    Response response = request(instanceResponse.getBaseUri() + "/topics/" + topic)
        .accept(expectedMediatype).get();
    if (expectFailure) {
      assertErrorResponse(Response.Status.NOT_FOUND, response,
                          Errors.TOPIC_NOT_FOUND_ERROR_CODE,
                          Errors.TOPIC_NOT_FOUND_MESSAGE,
                          expectedMediatype);
    } else {
      assertOKResponse(response, expectedMediatype);
      List<BinaryConsumerRecord> consumed = TestUtils.tryReadEntityOrLog(response, 
          new GenericType<List<BinaryConsumerRecord>>() {
          });
      assertEquals(0, consumed.size());
    }

    return instanceResponse.getBaseUri();
  }

  // Interface for converter from type used by Kafka producer (e.g. GenericRecord) to the type
  // actually consumed (e.g. JsonNode) so we can get both input and output in consistent form to
  // generate comparable data sets for validation
  protected static interface Converter {

    public Object convert(Object obj);
  }

  // This requires a lot of type info because we use the raw ProducerRecords used to work with
  // the Kafka producer directly (e.g. Object for GenericRecord+primitive for Avro) and the
  // consumed data type on the receiver (JsonNode, since the data has been converted to Json).
  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType extends ConsumerRecord<ClientK, ClientV>>
  void assertEqualsMessages(
      List<ProducerRecord<KafkaK, KafkaV>> records, // input messages
      List<RecordType> consumed, // output messages
      Converter converter) {

    // Since this is used for unkeyed messages, this can't rely on ordering of messages
    Map<Object, Integer> inputSetCounts = new HashMap<Object, Integer>();
    for (ProducerRecord<KafkaK, KafkaV> rec : records) {
      Object key = TestUtils.encodeComparable(
          (converter != null ? converter.convert(rec.key()) : rec.key())),
          value = TestUtils.encodeComparable(
              (converter != null ? converter.convert(rec.value()) : rec.value()));
      inputSetCounts.put(key,
                         (inputSetCounts.get(key) == null ? 0 : inputSetCounts.get(key)) + 1);
      inputSetCounts.put(value,
                         (inputSetCounts.get(value) == null ? 0 : inputSetCounts.get(value)) + 1);
    }
    Map<Object, Integer> outputSetCounts = new HashMap<Object, Integer>();
    for (ConsumerRecord<ClientK, ClientV> rec : consumed) {
      Object key = TestUtils.encodeComparable(rec.getKey()),
          value = TestUtils.encodeComparable(rec.getValue());
      outputSetCounts.put(
          key,
          (outputSetCounts.get(key) == null ? 0 : outputSetCounts.get(key)) + 1);
      outputSetCounts.put(
          value,
          (outputSetCounts.get(value) == null ? 0 : outputSetCounts.get(value)) + 1);
    }
    assertEquals(inputSetCounts, outputSetCounts);
  }

  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType extends ConsumerRecord<ClientK, ClientV>>
  void simpleConsumeMessages(
      String topicName,
      int offset,
      Integer count,
      List<ProducerRecord<KafkaK, KafkaV>> records,
      String accept,
      String responseMediatype,
      GenericType<List<RecordType>> responseEntityType,
      Converter converter) {

    Map<String, String> queryParams = new HashMap<String, String>();
    queryParams.put("offset", Integer.toString(offset));
    if (count != null) {
      queryParams.put("count", count.toString());
    }
    Response response = request("/topics/" + topicName + "/partitions/0/messages", queryParams)
        .accept(accept).get();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = TestUtils.tryReadEntityOrLog(response, responseEntityType);
    assertEquals(records.size(), consumed.size());

    assertEqualsMessages(records, consumed, converter);
  }


  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType extends ConsumerRecord<ClientK, ClientV>>
  void consumeMessagesV2(
          String groupName, String instanceName, String topic, List<ProducerRecord<KafkaK, KafkaV>> records,
          String accept, String responseMediatype,
          GenericType<List<RecordType>> responseEntityType,
          Converter converter) {

    Response subscriptionsResponse = request("/consumers/"+groupName+"/instances/"+instanceName+"/subscription").accept(Versions.KAFKA_V2_JSON).get();
    assertEquals("{\"topics\":[\""+topic+"\"]}", subscriptionsResponse.readEntity(String.class));


    Response response = request("/consumers/"+groupName+"/instances/"+instanceName+"/records")
             .accept(accept).get();

    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = TestUtils.tryReadEntityOrLog(response, responseEntityType);
    assertEquals(records.size(), consumed.size());
    assertEqualsMessages(records, consumed, converter);
  }



  protected <KafkaK, KafkaV, ClientK, ClientV, RecordType extends ConsumerRecord<ClientK, ClientV>>
  void consumeMessages(
      String instanceUri, String topic, List<ProducerRecord<KafkaK, KafkaV>> records,
      String accept, String responseMediatype,
      GenericType<List<RecordType>> responseEntityType,
      Converter converter) {
    Response response = request(instanceUri + "/topics/" + topic)
        .accept(accept).get();

    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = TestUtils.tryReadEntityOrLog(response, responseEntityType);
    assertEquals(records.size(), consumed.size());

    assertEqualsMessages(records, consumed, converter);
  }

  protected <K, V, RecordType extends ConsumerRecord<K, V>> void consumeForTimeout(
      String instanceUri, String topic, String accept, String responseMediatype,
      GenericType<List<RecordType>> responseEntityType) {
    long started = System.currentTimeMillis();
    Response response = request(instanceUri + "/topics/" + topic)
        .accept(accept).get();
    long finished = System.currentTimeMillis();
    assertOKResponse(response, responseMediatype);
    List<RecordType> consumed = TestUtils.tryReadEntityOrLog(response, responseEntityType);
    assertEquals(0, consumed.size());

    // Note that this is only approximate and really only works if you assume the read call has
    // a dedicated ConsumerWorker thread. Also note that we have to include the consumer
    // request timeout, the iterator timeout used for "peeking", and the backoff period, as well
    // as some extra slack for general overhead (which apparently mostly comes from running the
    // request and can be quite substantial).
    final int TIMEOUT = restConfig.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    final int TIMEOUT_SLACK =
        restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG)
        + restConfig.getInt(KafkaRestConfig.CONSUMER_ITERATOR_TIMEOUT_MS_CONFIG) + 500;
    long elapsed = finished - started;
    assertTrue(
        "Consumer request should not return before the timeout when no data is available",
        elapsed > TIMEOUT
    );
    assertTrue(
        "Consumer request should timeout approximately within the request timeout period",
        (elapsed - TIMEOUT) < TIMEOUT_SLACK
    );
  }

  protected void commitOffsets(String instanceUri) {
    Response response = request(instanceUri + "/offsets/")
        .post(Entity.entity(null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    // We don't verify offsets since they'll depend on how data gets distributed to partitions.
    // Just parse to check the output is formatted validly.
    List<TopicPartitionOffset> offsets =
            TestUtils.tryReadEntityOrLog(response, new GenericType<List<TopicPartitionOffset>>() {
        });
  }

  // Either topic or instance not found
  protected void consumeForNotFoundError(String instanceUri, String topic) {
    Response response = request(instanceUri + "/topics/" + topic)
        .get();
    assertErrorResponse(Response.Status.NOT_FOUND, response,
                        Errors.CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE,
                        Errors.CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
                        Versions.KAFKA_V1_JSON_BINARY);
  }

  protected void deleteConsumer(String instanceUri) {
    Response response = request(instanceUri).delete();
    assertErrorResponse(Response.Status.NO_CONTENT, response,
                        0, null, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
  }
}
