package org.etf.graph.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.etf.graph.core.Node;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility klasa za snimanje rezultata bojenja u JSON format.
 * Za razliku od GraphSerializer koji snima kompletan graf (sa susjedima),
 * ova klasa snima samo mapiranje čvor → boja i hromatski broj (minimalan broj boja).
 */
public class ColoringResultSerializer {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Snima rezultat bojenja u JSON fajl.
     */
    public static void saveColoringResult(List<Node> nodes, int chromaticNumber, String path) {
        try {
            // Konverzija List<Node> → ColoringResultData
            List<ColoringResultData.NodeColorPair> assignments = nodes.stream()
                    .map(node -> new ColoringResultData.NodeColorPair(node.id(), node.color()))
                    .collect(Collectors.toList());

            ColoringResultData data = new ColoringResultData(
                    chromaticNumber,
                    nodes.size(),
                    assignments
            );

            // Snimanje u JSON sa pretty printing-om
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(path), data);

        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Greška pri snimanju rezultata bojenja u JSON fajl na putanji: " + path, e);
        }
    }

}