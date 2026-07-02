package org.etf.graph.cli;

import java.io.Serial;

/**
 * Kontrolisani izuzetak koji signalizira da je korisnik zatražio pomoć (argument: --help).
 * Koristi se za uredno gašenje aplikacije bez logovanja greške.
 */
public class HelpRequestedException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public HelpRequestedException() {
        super();
    }

}