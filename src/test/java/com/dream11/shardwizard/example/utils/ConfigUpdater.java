package com.dream11.shardwizard.example.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ConfigUpdater {

  public static void updateSourceTypeInConfigFile(String configFilePath, String dbType)
      throws IOException {
    List<String> lines = Files.readAllLines(Paths.get(configFilePath));
    boolean found = false;

    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i).trim();
      if (line.startsWith("sourceType")) {
        // Replace the line
        lines.set(i, "sourceType = \"" + dbType + "\"");
        found = true;
        break;
      }
    }

    if (!found) {
      // If not found, add the key-value pair at the end
      lines.add("sourceType = \"" + dbType + "\"");
    }

    Files.write(Paths.get(configFilePath), lines);
  }
}
