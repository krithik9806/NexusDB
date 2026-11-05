package io.nexusdb.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusCoreModuleTest {
    @Test
    void moduleNameMatches() {
        assertEquals("nexus-core", NexusCoreModule.name());
    }
}

