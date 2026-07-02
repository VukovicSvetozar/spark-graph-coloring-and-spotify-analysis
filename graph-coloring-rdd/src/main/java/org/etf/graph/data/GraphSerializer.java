package org.etf.graph.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.etf.graph.core.Node;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Utility klasa za JSON serijalizaciju i deserijalizaciju grafova.
 * Koristi Jackson biblioteku za pretvaranje List<Node> u JSON format i obrnuto.
 */
public class GraphSerializer {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Snima listu čvorova u JSON fajl.
     */
    public static void saveAsJson(List<Node> nodes, String path) throws IllegalArgumentException {
        try {
            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(new OutputStreamWriter(
                            new FileOutputStream(path),
                            StandardCharsets.UTF_8), nodes);
        } catch (IOException e) {
            throw new IllegalArgumentException("Greška pri snimanju grafa u JSON fajl na putanji: " + path, e);
        }
    }

    /**
     * Učitava listu čvorova iz JSON fajla.
     */
    public static List<Node> loadFromJson(String path) throws IllegalArgumentException {
        try {
            return mapper.readValue(new File(path), new TypeReference<>() {
            });
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Fajl sa grafom nije pronađen na putanji: " + path);
        } catch (IOException e) {
            throw new IllegalArgumentException("Greška pri čitanju ili parsiranju JSON fajla na putanji: " + path + ". Detalji: " + e.getMessage(), e);
        }
    }

}