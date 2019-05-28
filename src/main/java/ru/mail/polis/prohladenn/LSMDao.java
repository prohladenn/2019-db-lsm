package ru.mail.polis.prohladenn;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.DAOFactory;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.SimpleFileVisitor;
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
    private Collection<FileTable> fileTables;
    private final boolean isSnapshot;
    private final Collection<String> snapshots;
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
            final long flushThreshold,
            boolean isSnapshot) throws IOException {
        assert flushThreshold >= 0L;
        this.base = base;
        this.flushThreshold = flushThreshold;
        this.isSnapshot = isSnapshot;
        this.memTable = new MemTable();
        this.fileTables = new ArrayList<>();
        this.snapshots = new ArrayList<>();
        this.generation = 0;
        Files.walkFileTree(
                base.toPath(),
                EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                1,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(
                            final Path path,
                            final BasicFileAttributes attrs) throws IOException {
                        final String fileName = path.getFileName().toString();
                        if (fileName.endsWith(SUFFIX)
                                && fileName.startsWith(PREFIX)) {
                            final int fileGen = Integer.valueOf(
                                    fileName.substring(
                                            PREFIX.length(),
                                            fileName.length() - SUFFIX.length()));
                            generation = Math.max(generation, fileGen + 1);
                            fileTables.add(new FileTable(path.toFile()));
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
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
        final Collection<Iterator<Cell>> filesIterators = new ArrayList<>(fileTables.size() + 1);

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

    private String flush(@NotNull final Iterator<Cell> iterator) throws IOException {
        if (!iterator.hasNext()) return "";
        final File tmp = new File(base, PREFIX + generation + TEMP);
        FileTable.write(iterator, tmp);
        final File dest = new File(base, PREFIX + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemTable();
        return dest.toPath().toString();
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
        final String res = flush(cellIterator(ByteBuffer.allocate(0)));
        fileTables.forEach(fileTable -> {
            try {
                Files.delete(fileTable.getPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        fileTables = new ArrayList<>();
        if (!res.isEmpty()) {
            fileTables.add(new FileTable(new File(res)));
            memTable = new MemTable();
        }
    }

    @Override
    public DAO snapshot() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)));
        final String snapshotPath = base.getPath() + "/snapshot" + snapshots.size();
        snapshots.add(snapshotPath);
        if (!new File(snapshotPath).mkdir()) {
            throw new IOException("Ошибка создания новой папки для снапшота");
        }
        Files.walkFileTree(
                base.toPath(),
                EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                1,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(
                            final Path path,
                            final BasicFileAttributes attrs) throws IOException {
                        final String fileName = path.getFileName().toString();
                        if (fileName.endsWith(SUFFIX)
                                && fileName.startsWith(PREFIX)) {
                            Files.createLink(Path.of(snapshotPath + "/" + fileName), path);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });

        return DAOFactory.create(new File(snapshotPath), true);
    }

    @Override
    public void close() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)));
        if (isSnapshot) {
            for (final String child : base.list()) {
                Files.delete(Paths.get(base + "/" + child));
            }
            Files.delete(base.toPath());
        }
    }
}
