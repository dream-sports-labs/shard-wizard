package com.dream11.shardwizard.example.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto {

  private String orderId;
  private String orderName;
  private String orderType;
  private String orderStatus;
  private String orderDate;
  private String orderTime;
  private double orderAmount;
  private String userId;
  private String roundId;
}
