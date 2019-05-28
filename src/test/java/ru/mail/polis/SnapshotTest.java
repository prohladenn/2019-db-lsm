package ru.mail.polis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Snapshot tests for {@link DAO} implementations
 *
 * @author Valery Kovshov
 */
class SnapshotTest extends TestBase {

    @Test
    void getAndClear(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            DAO snap = dao.snapshot();
            assertEquals(value, snap.get(key));
            assertEquals(value, dao.get(key));
        }

        for (String file : data.list()) {
            if (file.startsWith("snapshot")) {
                fail();
            }
        }
    }

    @Test
    void getAfterUpdate(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value1 = randomValue();
        final ByteBuffer value2 = randomValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value1);
            DAO snap = dao.snapshot();
            dao.upsert(key, value2);
            assertEquals(value1, snap.get(key));
            assertEquals(value2, dao.get(key));
        }
    }

    @Test
    void getAfterCompaction1(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKey();
        final int valueSize = 1024 * 128;
        final ByteBuffer value = randomBuffer(valueSize);
        final ByteBuffer value1 = randomBuffer(valueSize);
        final ByteBuffer value2 = randomBuffer(valueSize);
        final int keyCount = 1000;
        final ArrayList<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value1);
            DAO snap = dao.snapshot();
            for (ByteBuffer k : keys) {
                dao.upsert(k, value);
            }
            dao.upsert(key, value2);
            dao.compact();
            assertEquals(value1, snap.get(key));
            assertEquals(value2, dao.get(key));
        }
    }

    @Test
    void getAfterCompaction2(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKey();
        final int valueSize = 1024 * 128;
        final ByteBuffer value = randomBuffer(valueSize);
        final ByteBuffer value1 = randomBuffer(valueSize);
        final ByteBuffer value2 = randomBuffer(valueSize);
        final int keyCount = 1000;
        final ArrayList<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        try (DAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < keyCount / 2; i++) {
                dao.upsert(keys.get(i), value);
            }
            dao.upsert(key, value1);
            DAO snap = dao.snapshot();
            for (int i = keyCount / 2; i < keyCount; i++) {
                dao.upsert(keys.get(i), value);
            }
            dao.upsert(key, value2);
            dao.compact();
            assertEquals(value1, snap.get(key));
            assertEquals(value2, dao.get(key));
        }
    }

    @Test
    void manySnap(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value1 = randomValue();
        final ByteBuffer value2 = randomValue();
        final ByteBuffer value3 = randomValue();
        final ByteBuffer value4 = randomValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value1);
            DAO snap1 = dao.snapshot();
            dao.upsert(key, value2);
            DAO snap2 = dao.snapshot();
            dao.upsert(key, value3);
            DAO snap3 = dao.snapshot();
            dao.upsert(key, value4);
            assertEquals(value1, snap1.get(key));
            assertEquals(value2, snap2.get(key));
            assertEquals(value3, snap3.get(key));
            assertEquals(value4, dao.get(key));
        }
    }
}
