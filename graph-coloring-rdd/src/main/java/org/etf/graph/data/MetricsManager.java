package org.etf.graph.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * MetricsManager je zadužen za serijalizaciju i izvoz svih metrika i rezultata Spark Jobova
 * (AverageDegreeResult, ColoringResult, ValidationResult) u JSON format.
 */
public class MetricsManager {

    private static final Logger LOG = LogManager.getLogger(MetricsManager.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Generički snima bilo koji Java objekat (metriku) u JSON fajl.
     */
    public static <T> void save(T metrics, String path, String suffix) {

        if (path == null || path.trim().isEmpty()) {
            LOG.warn("Putanja za izvoz metrika nije definisana. Preskače se snimanje metrika.");
            return;
        }

        String finalPath = path + suffix + ".json";

        try {
            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(new File(finalPath), metrics);
            LOG.info("Metrike su uspješno izvezene u: {}", finalPath);
        } catch (IOException e) {
            LOG.error("Greška pri snimanju metrika u JSON fajl na putanji: {}", finalPath, e);
        }

    }

}