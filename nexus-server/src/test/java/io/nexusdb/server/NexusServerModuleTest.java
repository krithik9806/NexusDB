package io.nexusdb.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusServerModuleTest {
    @Test
    void moduleNameMatches() {
        assertEquals("nexus-server", NexusServerModule.name());
    }
}

