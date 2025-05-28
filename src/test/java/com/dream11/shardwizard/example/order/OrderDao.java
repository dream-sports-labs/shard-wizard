package com.dream11.shardwizard.example.order;

import io.reactivex.Single;

public interface OrderDao {

  Single<CreateOrderResponse> create(OrderDto orderDto);

  Single<OrderDto> get(String orderId);
}
