package ru.mail.polis.prohladenn;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public final class LSMDao implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private Table memTable;
    private final long flushThreshold;
    private final File base;
    private int generation;
    private final Collection<FileTable> fileTables;

    public LSMDao(
            final File base,
            final long flushThreshold) throws IOException {
        memTable = new MemTable();
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        this.base = base;
        fileTables = new ArrayList<>();
        Files.walkFileTree(base.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                fileTables.add(new FileTable(file.toFile()));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final ArrayList<Iterator<Cell>> filesIterators = new ArrayList<>();
        for (final FileTable fileTable : fileTables) {
            filesIterators.add(fileTable.iterator(from));
        }
        filesIterators.add(memTable.iterator(from));
        final Iterator<Cell> cells =
                Iters.collapseEquals(
                        Iterators.mergeSorted(filesIterators, Cell.COMPARATOR),
                        Cell::getKey);
        final Iterator<Cell> alive =
                Iterators.filter(
                        cells,
                        cell -> {
                            assert cell != null;
                            return !cell.getValue().isRemoved();
                        });
        return Iterators.transform(
                alive,
                cell -> {
                    assert cell != null;
                    return Record.of(cell.getKey(), cell.getValue().getData());
                });
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File tmp = new File(base, generation + TEMP);
        FileTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(base, generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemTable();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
