package io.nexusdb.benchmarks;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusBenchmarksModuleTest {
    @Test
    void moduleNameMatches() {
        assertEquals("nexus-benchmarks", NexusBenchmarksModule.name());
    }
}

