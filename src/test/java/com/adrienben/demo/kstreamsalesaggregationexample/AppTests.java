package com.adrienben.demo.kstreamsalesaggregationexample;

import com.adrienben.demo.kstreamsalesaggregationexample.domain.Sale;
import com.adrienben.demo.kstreamsalesaggregationexample.domain.Sales;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.adrienben.demo.kstreamsalesaggregationexample.config.StreamConfig.AGGREGATED_SALES_TOPIC;
import static com.adrienben.demo.kstreamsalesaggregationexample.config.StreamConfig.SALES_TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasKey;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@SpringBootTest
@EmbeddedKafka(
		topics = { SALES_TOPIC, AGGREGATED_SALES_TOPIC },
		ports = { 19092 }
)
@TestPropertySource(properties = {
		"spring.kafka.bootstrap-servers=localhost:19092",
		"app.window.duration=0"
})
public class AppTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private ObjectMapper mapper;

	@Test
	void integrationTestWithoutWindow() throws ExecutionException, InterruptedException {
		var saleProducer = createProducer(new StringSerializer(), new JsonSerializer<Sale>(mapper));
		var salesConsumer = createConsumer(AGGREGATED_SALES_TOPIC, new StringDeserializer(), new JsonDeserializer<>(Sales.class, mapper, false));

		var sale = new Sale("F905", 10f);

		saleProducer.send(new ProducerRecord<>(SALES_TOPIC, sale.getShopId(), sale)).get();
		var first = KafkaTestUtils.getSingleRecord(salesConsumer, AGGREGATED_SALES_TOPIC, Duration.ofSeconds(5).toMillis());

		saleProducer.send(new ProducerRecord<>(SALES_TOPIC, sale.getShopId(), sale)).get();
		var second = KafkaTestUtils.getSingleRecord(salesConsumer, AGGREGATED_SALES_TOPIC, Duration.ofSeconds(5).toMillis());

		saleProducer.send(new ProducerRecord<>(SALES_TOPIC, sale.getShopId(), sale)).get();
		var third = KafkaTestUtils.getSingleRecord(salesConsumer, AGGREGATED_SALES_TOPIC, Duration.ofSeconds(5).toMillis());

		assertAll(
				() -> assertThat(first, hasKey("F905")),
				() -> assertThat(first, hasValue(new Sales("F905", 10f))),
				() -> assertThat(second, hasKey("F905")),
				() -> assertThat(second, hasValue(new Sales("F905", 20f))),
				() -> assertThat(third, hasKey("F905")),
				() -> assertThat(third, hasValue(new Sales("F905", 30f)))
		);
	}

	private <K, V> Producer<K, V> createProducer(
			Serializer<K> keySerializer,
			Serializer<V> valueSerializer
	) {
		var producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		var defaultKafkaProducerFactory = new DefaultKafkaProducerFactory<>(producerProps, keySerializer, valueSerializer);
		return defaultKafkaProducerFactory.createProducer();
	}

	private <K, V> Consumer<K, V> createConsumer(
			String topic,
			Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer
	) {
		var consumerProps = KafkaTestUtils.consumerProps("integration-test", "true", embeddedKafka);
		var defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, keyDeserializer, valueDeserializer);
		var consumer = defaultKafkaConsumerFactory.createConsumer();
		consumer.subscribe(Set.of(topic));
		return consumer;
	}
}
