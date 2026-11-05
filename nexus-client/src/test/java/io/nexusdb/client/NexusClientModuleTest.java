package io.nexusdb.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusClientModuleTest {
    @Test
    void moduleNameMatches() {
        assertEquals("nexus-client", NexusClientModule.name());
    }
}

