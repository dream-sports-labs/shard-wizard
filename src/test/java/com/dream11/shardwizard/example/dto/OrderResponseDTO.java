package com.dream11.shardwizard.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrderResponseDTO {

  String orderId;
  long userId;
}
