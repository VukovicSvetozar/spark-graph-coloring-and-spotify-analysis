package org.etf.graph.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.jetbrains.annotations.NotNull;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Predstavlja čvor neusmjerenog, netežinskog grafa.
 * Svaki čvor ima ID, listu susjeda i boju.
 */
@JsonPropertyOrder({"id", "neighbors", "color"})
public record Node(

        @JsonProperty("id")
        int id,

        @JsonProperty("neighbors")
        Set<Integer> neighbors,

        @JsonProperty("color")
        int color

) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Canonical constructor.
     * Provjerava podatke i normalizuje stanje objekta.
     */
    @JsonCreator
    public Node {
        Set<Integer> normalized = (neighbors == null) ? Set.of() : neighbors;
        normalized = normalized.stream()
                .filter(n -> n != id)
                .collect(Collectors.toSet());
        neighbors = Set.copyOf(normalized);
    }

    /**
     * Kreira čvor sa zadatim ID-jem bez susjeda i neobojen (-1).
     */
    public Node(int id) {
        this(id, Set.of(), -1);
    }

    /**
     * Vraća nepromjenjiv skup susjeda čvora.
     */
    public Set<Integer> neighbors() {
        return neighbors;
    }

    /**
     * Vraća novi čvor sa dodatim susjedom.
     */
    public Node withAddedNeighbor(int neighborId) {
        if (neighborId == id || neighbors.contains(neighborId)) {
            return this;
        }
        Set<Integer> newNeighbors = new HashSet<>(neighbors);
        newNeighbors.add(neighborId);
        return new Node(id, newNeighbors, color);
    }

    /**
     * Vraća novi čvor sa zamijenjenom kompletnom listom susjeda.
     * Koristi se za složene mutacije u inkrementalnom bojenju.
     */
    public Node withNeighbors(Set<Integer> newNeighbors) {
        return new Node(id, newNeighbors, color);
    }

    /**
     * Vraća novi čvor sa promijenjenom bojom.
     */
    public Node withColor(int newColor) {
        return (newColor == color) ? this : new Node(id, neighbors, newColor);
    }

    /**
     * Vraća broj susjeda čvora.
     */
    public int degree() {
        return neighbors.size();
    }

    @Override
    public @NotNull String toString() {
        return String.format("Node{id=%d, color=%d, neighbors=%s}", id, color, neighbors);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Node node && id == node.id);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }

}