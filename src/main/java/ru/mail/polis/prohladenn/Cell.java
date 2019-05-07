package ru.mail.polis.prohladenn;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell {

    public static final Comparator<Cell> COMPARATOR =
            Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue);

    private final ByteBuffer key;
    private final Value value;

    public Cell(final ByteBuffer key, final Value value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }
}
