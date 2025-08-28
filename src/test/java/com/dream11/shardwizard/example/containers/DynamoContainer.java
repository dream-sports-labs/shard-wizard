package com.dream11.shardwizard.example.containers;

import static com.dream11.shardwizard.example.helpers.Constants.DYNAMODB_IMAGE;

import java.net.URI;
import org.testcontainers.containers.GenericContainer;

public class DynamoContainer {

  public static GenericContainer<?> create(int exposedPort) {
    int defaultPort = 8000;

    GenericContainer<?> dynamoContainer =
        new GenericContainer<>(DYNAMODB_IMAGE)
            .withExposedPorts(defaultPort)
            .withCommand("-jar DynamoDBLocal.jar -sharedDb");

    dynamoContainer.setPortBindings(
        java.util.Collections.singletonList(exposedPort + ":" + defaultPort));

    dynamoContainer.start();

    System.out.println("DynamoDB Local container is running!");
    System.out.println("Endpoint: " + getEndpoint(dynamoContainer, defaultPort));

    return dynamoContainer;
  }

  public static String getEndpoint(GenericContainer<?> container, int internalPort) {
    return "http://" + container.getHost() + ":" + container.getMappedPort(internalPort);
  }

  public static URI getEndpointURI(GenericContainer<?> container, int internalPort) {
    return URI.create(getEndpoint(container, internalPort));
  }
}
