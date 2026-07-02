package org.etf.graph.incremental;

/**
 * Tipovi operacija za inkrementalne promjene u grafu (Zadatak 6).
 */
public enum GraphChangeType {
    /**
     * Dodavanje novog čvora sa inicijalnim susjedima
     */
    ADD_NODE,

    /**
     * Dodavanje nove ivice između dva postojeća čvora
     */
    ADD_EDGE,

    /**
     * Uklanjanje čvora i svih njegovih veza
     */
    REMOVE_NODE,

    /**
     * Uklanjanje ivice između dva čvora (oba ostaju u grafu)
     */
    REMOVE_EDGE
}