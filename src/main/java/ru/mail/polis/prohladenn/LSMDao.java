package ru.mail.polis.prohladenn;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;

public final class LSMDao implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static final String PREFIX = "DB";

    private final long flushThreshold;
    private final File base;
    private final Collection<FileTable> fileTables;
    private Table memTable;
    private int generation;

    /**
     * Creates persistence LSMDao.
     *
     * @param base           folder with FileTable
     * @param flushThreshold threshold memTable's size
     * @throws IOException if an I/O error occurred
     */
    public LSMDao(
            final File base,
            final long flushThreshold) throws IOException {
        assert flushThreshold >= 0L;
        this.base = base;
        this.flushThreshold = flushThreshold;
        this.memTable = new MemTable();
        this.fileTables = new ArrayList<>();
        Files.walkFileTree(
                base.toPath(),
                EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                1,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(
                            final Path path,
                            final BasicFileAttributes attrs) throws IOException {
                        if (path.getFileName().toString().endsWith(SUFFIX)
                                && path.getFileName().toString().startsWith(PREFIX)) {
                            fileTables.add(new FileTable(path.toFile()));
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
        this.generation = fileTables.size() - 1;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(
                cellIterator(from),
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @NotNull
    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) throws IOException {
        final Collection<Iterator<Cell>> filesIterators = new ArrayList<>();

        // SSTables iterators
        for (final FileTable fileTable : fileTables) {
            filesIterators.add(fileTable.iterator(from));
        }

        // MemTable iterator
        filesIterators.add(memTable.iterator(from));
        final Iterator<Cell> cells = Iters.collapseEquals(Iterators.mergeSorted(filesIterators, Cell.COMPARATOR),
                Cell::getKey);
        final Iterator<Cell> alive =
                Iterators.filter(
                        cells,
                        cell -> !cell.getValue().isRemoved());
        return alive;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush(memTable.iterator(ByteBuffer.allocate(0)));
        }
    }

    private void flush(@NotNull final Iterator<Cell> iterator) throws IOException {
        final File tmp = new File(base, PREFIX + generation + TEMP);
        FileTable.write(iterator, tmp);
        final File dest = new File(base, PREFIX + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemTable();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush(memTable.iterator(ByteBuffer.allocate(0)));
        }
    }

    @Override
    public void compact() throws IOException {
        flush(cellIterator(ByteBuffer.allocate(0)));
        fileTables.forEach(fileTable -> {
            try {
                Files.delete(fileTable.getPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void close() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)));
    }
}
