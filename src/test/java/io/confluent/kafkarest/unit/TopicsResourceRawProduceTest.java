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

package io.confluent.kafkarest.unit;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.entities.*;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import io.confluent.rest.exceptions.RestServerErrorException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class TopicsResourceRawProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private MetadataObserver mdObserver;
  private ProducerPool producerPool;
  private DefaultKafkaRestContext ctx;

  private static final String topicName = "topic1";

  private List<RawTopicProduceRecord> produceRecordsOnlyValues;
  private List<RawTopicProduceRecord> produceRecordsWithKeys;
  private List<RawTopicProduceRecord> produceRecordsWithPartitions;
  private List<RawTopicProduceRecord> produceRecordsWithPartitionsAndKeys;
  private List<RawTopicProduceRecord> produceRecordsWithNullValues;
  private List<RecordMetadataOrException> produceResults;
  private List<PartitionOffset> offsetResults;

  private List<RawTopicProduceRecord> produceExceptionData;
  private List<RecordMetadataOrException> produceGenericExceptionResults;
  private List<RecordMetadataOrException> produceKafkaExceptionResults;
  private List<PartitionOffset> kafkaExceptionResults;
  private List<RecordMetadataOrException> produceKafkaRetriableExceptionResults;
  private List<PartitionOffset> kafkaRetriableExceptionResults;

  private static final String exceptionMessage = "Error message";

  public TopicsResourceRawProduceTest() throws RestConfigException {
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new DefaultKafkaRestContext(config, mdObserver, producerPool, null, null, null, null);

    addResource(new TopicsResource(ctx));

    produceRecordsOnlyValues = Arrays.asList(
        new RawTopicProduceRecord("value"),
        new RawTopicProduceRecord("value2")
    );
    produceRecordsWithKeys = Arrays.asList(
        new RawTopicProduceRecord("key", "value"),
        new RawTopicProduceRecord("key2", "value2")
    );
    produceRecordsWithPartitions = Arrays.asList(
        new RawTopicProduceRecord("value", 0),
        new RawTopicProduceRecord("value2", 0)
    );
    produceRecordsWithPartitionsAndKeys = Arrays.asList(
        new RawTopicProduceRecord("key", "value", 0),
        new RawTopicProduceRecord("key2", "value2", 0)
    );
    produceRecordsWithNullValues = Arrays.asList(
        new RawTopicProduceRecord("key", (String)null),
        new RawTopicProduceRecord("key2", (String)null)
    );

    TopicPartition tp0 = new TopicPartition(topicName, 0);
    produceResults = Arrays.asList(
        new RecordMetadataOrException(new RecordMetadata(tp0, 0, 0, 0, 0, 1, 1), null),
        new RecordMetadataOrException(new RecordMetadata(tp0, 0, 1, 0, 0, 1, 1), null)
    );

    offsetResults = Arrays.asList(
        new PartitionOffset(0, 0L, null, null),
        new PartitionOffset(0, 1L, null, null)
    );

    produceExceptionData = Arrays.asList(
        new RawTopicProduceRecord((String) null, (String) null)
    );

    produceGenericExceptionResults = Arrays.asList(
        new RecordMetadataOrException(null, new Exception(exceptionMessage))
    );

    produceKafkaExceptionResults = Arrays.asList(
        new RecordMetadataOrException(null, new KafkaException(exceptionMessage))
    );
    kafkaExceptionResults = Arrays.asList(
        new PartitionOffset(null, null, Errors.KAFKA_ERROR_ERROR_CODE, exceptionMessage)
    );

    produceKafkaRetriableExceptionResults = Arrays.asList(
        new RecordMetadataOrException(null, new RetriableException(exceptionMessage) {
        })
    );
    kafkaRetriableExceptionResults = Arrays.asList(
        new PartitionOffset(null, null, Errors.KAFKA_RETRIABLE_ERROR_ERROR_CODE,
                            exceptionMessage)
    );
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(mdObserver, producerPool);
  }

  private <K, V> Response produceToTopic(String topic, String acceptHeader, String requestMediatype,
                                         EmbeddedFormat recordFormat,
                                         List<? extends TopicProduceRecord<K, V>> records,
                                         final List<RecordMetadataOrException> results) {
    final TopicProduceRequest request = new TopicProduceRequest();
    request.setRecords(records);
    final Capture<ProducerPool.ProduceRequestCallback>
        produceCallback =
        new Capture<ProducerPool.ProduceRequestCallback>();
    producerPool.produce(EasyMock.eq(topic),
                         EasyMock.eq((Integer) null),
                         EasyMock.eq(recordFormat),
                         EasyMock.<SchemaHolder>anyObject(),
                         EasyMock.<Collection<? extends ProduceRecord<K, V>>>anyObject(),
                         EasyMock.capture(produceCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        if (results == null) {
          throw new Exception();
        } else {
          produceCallback.getValue().onCompletion((Integer) null, (Integer) null, results);
        }
        return null;
      }
    });
    EasyMock.replay(mdObserver, producerPool);

    Response response = request("/topics/" + topic, acceptHeader)
        .post(Entity.entity(request, requestMediatype));

    EasyMock.verify(mdObserver, producerPool);

    return response;
  }

  private void testProduceToTopicSuccess(List<RawTopicProduceRecord> records) {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_RAW) {
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.RAW, records, produceResults);
        assertOKResponse(rawResponse, mediatype.expected);
        ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

        assertEquals(offsetResults, response.getOffsets());
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToTopicOnlyValues() {
    testProduceToTopicSuccess(produceRecordsOnlyValues);
  }

  @Test
  public void testProduceToTopicByKey() {
    testProduceToTopicSuccess(produceRecordsWithKeys);
  }

  @Test
  public void testProduceToTopicByPartition() {
    testProduceToTopicSuccess(produceRecordsWithPartitions);
  }

  @Test
  public void testProduceToTopicWithPartitionAndKey() {
    testProduceToTopicSuccess(produceRecordsWithPartitionsAndKeys);
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    testProduceToTopicSuccess(produceRecordsWithNullValues);
  }

  @Test
  public void testProduceToTopicFailure() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_RAW) {
        // null offsets triggers a generic exception
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.RAW,
                           produceRecordsWithKeys, null);
        assertErrorResponse(Response.Status.INTERNAL_SERVER_ERROR, rawResponse,
                            mediatype.expected);

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }


  private void testProduceToTopicException(List<RecordMetadataOrException> produceResults,
                                           List<PartitionOffset> produceExceptionResults) {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_RAW) {
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.RAW, produceExceptionData, produceResults);

        if (produceExceptionResults == null) {
          assertErrorResponse(
              Response.Status.INTERNAL_SERVER_ERROR, rawResponse,
              RestServerErrorException.DEFAULT_ERROR_CODE, Errors.UNEXPECTED_PRODUCER_EXCEPTION,
              mediatype.expected
          );
        } else {
          assertOKResponse(rawResponse, mediatype.expected);
          ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);
          assertEquals(produceExceptionResults, response.getOffsets());
          assertEquals(null, response.getKeySchemaId());
          assertEquals(null, response.getValueSchemaId());
        }

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToTopicGenericException() {
    // No results expected since a non-Kafka exception should cause an HTTP-level error
    testProduceToTopicException(produceGenericExceptionResults, null);
  }

  @Test
  public void testProduceToTopicKafkaException() {
    testProduceToTopicException(produceKafkaExceptionResults, kafkaExceptionResults);
  }

  @Test
  public void testProduceToTopicKafkaRetriableException() {
    testProduceToTopicException(produceKafkaRetriableExceptionResults,
                                kafkaRetriableExceptionResults);
  }
}
