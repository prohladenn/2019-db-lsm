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
import java.util.EnumSet;
import java.util.Iterator;

public final class LSMDao implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private Table memTable;
    private final long flushThreshold;
    private final File base;
    private int generation;
    private final Collection<FileTable> fileTables;

    /**
     * My NoSQL DAO.
     *
     * @param base           directory of DB
     * @param flushThreshold maxsize of @memTable
     * @throws IOException If an IO error occurs
     */
    public LSMDao(
            final File base,
            final long flushThreshold) throws IOException {
        memTable = new MemTable();
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        this.base = base;
        fileTables = new ArrayList<>();
        Files.walkFileTree(base.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                if (path.getFileName().toString().endsWith(SUFFIX)) {
                    fileTables.add(new FileTable(path.toFile()));
                    generation++;
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
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
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) throws IOException {
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
    public void compact() throws IOException {
        String string = "Ща всё будет";
        System.out.println(string);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
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
