package com.adrienben.demo.kstreamsalesaggregationexample.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sales {
	private String shopId;
	private Float amount;
	private LocalDateTime periodStart;
	private LocalDateTime periodEnd;

	public Sales(String shopId, Float amount) {
		this(shopId, amount, null, null);
	}
}
