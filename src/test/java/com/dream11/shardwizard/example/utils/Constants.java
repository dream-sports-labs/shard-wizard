package com.dream11.shardwizard.example.utils;

import org.testcontainers.utility.DockerImageName;

public class Constants {

  public static final DockerImageName DYNAMODB_IMAGE =
      DockerImageName.parse("amazon/dynamodb-local:latest");

  public static final String TABLE_NAME = "Orders";
}
