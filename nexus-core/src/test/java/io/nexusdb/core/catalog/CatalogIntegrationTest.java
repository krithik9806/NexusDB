package io.nexusdb.core.catalog;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.TableIterator;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.storage.DiskManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void createTableInsertTenThousandRowsReopenReloadAndScanAll() {
        Path dbFile = tempDir.resolve("nexus_phase1.db");
        final int rowCount = 10_000;

        Schema ordersSchema = new Schema(List.of(
                Column.primaryKey("order_id", DataType.INT),
                Column.varchar("customer_name", 64, false),
                new Column("amount_cents", DataType.BIGINT, false),
                new Column("tax_rate", DataType.DOUBLE, true),
                new Column("shipped", DataType.BOOLEAN, false)));

        try (DiskManager dm = DiskManager.open(dbFile, 4096);
             BufferPoolManager bpm = new BufferPoolManager(32, dm)) {

            Catalog catalog = Catalog.open(bpm);
            assertTrue(dm.getCatalogMetaPageId() > 0);

            TableMetadata orders = catalog.createTable("orders", ordersSchema);
            assertNotNull(orders);
            assertEquals("orders", orders.getName());
            assertEquals(ordersSchema, orders.getSchema());

            // Also register an index metadata entry to verify index persistence across reopen
            IndexMetadata idx = catalog.registerIndex("orders", "idx_orders_customer", "customer_name", 99, false);
            assertEquals("idx_orders_customer", idx.indexName());

            TableHeap heap = orders.getTableHeap();
            for (int i = 0; i < rowCount; i++) {
                Tuple row = new Tuple(
                        Value.ofInt(i),
                        Value.ofVarchar("customer_" + i),
                        Value.ofBigInt(1500L + i),
                        (i % 5 == 0) ? Value.NULL : Value.ofDouble(0.0825),
                        Value.ofBoolean((i & 1) == 0));
                heap.insert(row);
            }
        }

        // Reopen DiskManager, BufferPoolManager, and Catalog from disk and verify all 10k rows
        try (DiskManager dm = DiskManager.open(dbFile, 4096);
             BufferPoolManager bpm = new BufferPoolManager(16, dm)) {

            Catalog reopenedCatalog = Catalog.open(bpm);
            assertEquals(List.of("orders"), reopenedCatalog.listTableNames());

            TableMetadata reloadedOrders = reopenedCatalog.getTable("ORDERS"); // case-insensitive lookup
            assertNotNull(reloadedOrders);
            assertEquals(ordersSchema, reloadedOrders.getSchema());
            assertEquals(1, reloadedOrders.getIndexes().size());
            assertEquals("idx_orders_customer", reloadedOrders.getIndexes().getFirst().indexName());

            int scanned = 0;
            try (TableIterator it = reloadedOrders.getTableHeap().scan()) {
                while (it.hasNext()) {
                    Tuple row = it.next();
                    assertEquals(Value.ofInt(scanned), row.getValue(0));
                    assertEquals(Value.ofVarchar("customer_" + scanned), row.getValue(1));
                    assertEquals(Value.ofBigInt(1500L + scanned), row.getValue(2));
                    if (scanned % 5 == 0) {
                        assertTrue(row.getValue(3).isNull());
                    } else {
                        assertEquals(Value.ofDouble(0.0825), row.getValue(3));
                    }
                    assertEquals(Value.ofBoolean((scanned & 1) == 0), row.getValue(4));
                    scanned++;
                }
            }
            assertEquals(rowCount, scanned);
        }
    }

    @Test
    void createMultipleTablesDropTableAndValidateIdentifiers() {
        Path dbFile = tempDir.resolve("catalog_ddl.db");
        Schema simpleSchema = new Schema(List.of(
                Column.primaryKey("id", DataType.INT),
                Column.varchar("label", 32, true)));

        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(8, dm)) {

            Catalog catalog = Catalog.open(bpm);

            // Invalid SQL identifiers are rejected
            assertThrows(IllegalArgumentException.class, () -> catalog.createTable("123bad", simpleSchema));
            assertThrows(IllegalArgumentException.class, () -> catalog.createTable("bad-name", simpleSchema));
            assertThrows(IllegalArgumentException.class, () -> catalog.createTable("", simpleSchema));

            // Create 20 tables to span multiple 512-byte catalog pages
            for (int i = 0; i < 20; i++) {
                catalog.createTable("table_" + i, simpleSchema);
            }
            assertEquals(20, catalog.listTables().size());

            // Duplicate table name rejected
            assertThrows(IllegalArgumentException.class, () -> catalog.createTable("TABLE_0", simpleSchema));

            // Drop half of the tables and verify their heap pages are returned to the free list
            for (int i = 0; i < 20; i += 2) {
                assertTrue(catalog.dropTable("table_" + i));
                assertFalse(catalog.dropTable("table_" + i));
            }
            assertEquals(10, catalog.listTables().size());
            assertTrue(bpm.getPageAllocator().getFreePageCount() >= 10);
        }

        // Reopen and verify only odd-numbered tables remain
        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(8, dm)) {
            Catalog reopened = Catalog.open(bpm);
            assertEquals(10, reopened.listTables().size());
            for (int i = 0; i < 20; i++) {
                if (i % 2 == 0) {
                    assertNull(reopened.getTable("table_" + i));
                } else {
                    assertNotNull(reopened.getTable("table_" + i));
                }
            }
        }
    }
}
