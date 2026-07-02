package org.etf.graph.core;

/**
 * Jednostavna štoperica/tajmer za mjerenje vremena izvršavanja.
 */
public class Timer {

    private long startTime = -1;
    private long endTime = -1;
    private boolean running = false;

    public void start() {
        if (running) {
            throw new IllegalStateException("Štoperica je već pokrenuta.");
        }
        running = true;
        startTime = System.nanoTime();
    }

    public void stop() {
        if (!running) {
            throw new IllegalStateException("Štoperica nije pokrenuta.");
        }
        endTime = System.nanoTime();
        running = false;
    }

    public long getElapsedMillis() {
        ensureStopped();
        return (endTime - startTime) / 1_000_000;
    }

    private void ensureStopped() {
        if (running)
            throw new IllegalStateException("Štoperica je još uvijek aktivna.");
        if (startTime < 0 || endTime < 0)
            throw new IllegalStateException("Štoperica nikada nije pokrenuta ili zaustavljena.");
    }

    @Override
    public String toString() {
        if (running)
            return "Štoperica je aktivna...";

        return String.format("%d ms (%.3f s)",
                getElapsedMillis(),
                getElapsedMillis() / 1000.0
        );
    }

}