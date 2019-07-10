package com.adrienben.demo.kstreamsalesaggregationexample.config;

import com.adrienben.demo.kstreamsalesaggregationexample.domain.Sale;
import com.adrienben.demo.kstreamsalesaggregationexample.domain.Sales;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@Configuration
@EnableKafkaStreams
public class StreamConfig {

	public static final String SALES_TOPIC = "sales";
	public static final String AGGREGATED_SALES_TOPIC = "aggregated_sales";
	private static final String AMOUNT_STORE_NAME = "amount_store";
	private static final String WINDOWED_AMOUNT_STORE_NAME = "windowed_amount_store";
	private static final String WINDOWED_AMOUNT_SUPPRESS_NODE_NAME = "windowed_amount_suppress";

	private final Duration windowDuration;

	private final ObjectMapper mapper;
	private final Serde<String> stringSerde;
	private final Serde<Float> floatSerde;
	private final Serde<Sale> saleSerde;
	private final Consumed<String, Sale> saleConsumed;
	private final Serde<Sales> salesSerde;
	private final Produced<String, Sales> salesProduced;

	public StreamConfig(
			@Value("${app.window.duration}") Duration windowDuration,
			ObjectMapper mapper
	) {
		this.windowDuration = windowDuration;
		this.mapper = mapper;
		this.stringSerde = Serdes.String();
		this.floatSerde = Serdes.Float();
		this.saleSerde = jsonSerde(Sale.class);
		this.saleConsumed = Consumed.with(stringSerde, saleSerde);
		this.salesSerde = jsonSerde(Sales.class);
		this.salesProduced = Produced.with(stringSerde, salesSerde);
	}

	@Bean
	public KStream<String, Sales> kStream(StreamsBuilder streamBuilder) {
		var salesByShop = streamBuilder.stream(SALES_TOPIC, saleConsumed).groupByKey();
		var aggregatedSalesByShop = aggregate(salesByShop);
		aggregatedSalesByShop.to(AGGREGATED_SALES_TOPIC, salesProduced);
		return aggregatedSalesByShop;
	}

	private KStream<String, Sales> aggregate(KGroupedStream<String, Sale> salesByShop) {
		if (windowDuration.isZero()) {
			return salesByShop
					.aggregate(this::initialize, this::aggregateAmount, materializedAsPersistentStore(AMOUNT_STORE_NAME, stringSerde, floatSerde))
					.toStream()
					.mapValues(Sales::new);
		}

		return salesByShop.windowedBy(TimeWindows.of(windowDuration).grace(Duration.ZERO))
				.aggregate(this::initialize, this::aggregateAmount, materializedAsWindowStore(WINDOWED_AMOUNT_STORE_NAME, stringSerde, floatSerde))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName(WINDOWED_AMOUNT_SUPPRESS_NODE_NAME))
				.toStream()
				.map((key, aggregatedAmount) -> {
					var start = LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault());
					var end = LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault());
					return KeyValue.pair(key.key(), new Sales(key.key(), aggregatedAmount, start, end));
				});
	}

	private Float initialize() {
		return 0f;
	}

	private Float aggregateAmount(String key, Sale sale, Float aggregatedAmount) {
		return aggregatedAmount + sale.getAmount();
	}

	private <T> Serde<T> jsonSerde(Class<T> targetClass) {
		return Serdes.serdeFrom(
				new JsonSerializer<>(mapper),
				new JsonDeserializer<>(targetClass, mapper, false)
		);
	}

	private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedAsPersistentStore(
			String storeName,
			Serde<K> keySerde,
			Serde<V> valueSerde
	) {
		return Materialized.<K, V>as(Stores.persistentKeyValueStore(storeName))
				.withKeySerde(keySerde)
				.withValueSerde(valueSerde);
	}

	private <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materializedAsWindowStore(
			String storeName,
			Serde<K> keySerde,
			Serde<V> valueSerde
	) {
		return Materialized.<K, V>as(Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false))
				.withKeySerde(keySerde)
				.withValueSerde(valueSerde);
	}
}
