package com.dream11.shardwizard.example.containers;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

public class LocalStackS3Container {

  public static LocalStackContainer create() {
    LocalStackContainer localstack =
        new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withServices(LocalStackContainer.Service.S3)
            .withExposedPorts(4566)
            .withCreateContainerCmdModifier(
                cmd ->
                    cmd.withHostConfig(
                        new HostConfig()
                            .withPortBindings(
                                new PortBinding(
                                    Ports.Binding.bindPort(4566), new ExposedPort(4566)))));
    localstack.start();
    return localstack;
  }
}
