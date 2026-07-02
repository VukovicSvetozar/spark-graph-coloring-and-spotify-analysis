package org.etf.graph.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;

/**
 * DTO za snimanje rezultata bojenja grafa.
 * Ovaj format sadrži samo osnovne informacije o bojenju:
 *  - Hromatski broj (minimalan broj boja)
 *  - Mapiranje čvor → boja
 */
@JsonPropertyOrder({"chromaticNumber", "nodeCount", "colorAssignments"})
public record ColoringResultData(

        @JsonProperty("chromaticNumber")
        int chromaticNumber,

        @JsonProperty("nodeCount")
        int nodeCount,

        @JsonProperty("colorAssignments")
        List<NodeColorPair> colorAssignments
) {

    /**
     * Predstavlja jedno bojenje: ID čvora → boja.
     */
    @JsonPropertyOrder({"nodeId", "color"})
    public record NodeColorPair(
            @JsonProperty("nodeId") int nodeId,
            @JsonProperty("color") int color
    ) {}

}