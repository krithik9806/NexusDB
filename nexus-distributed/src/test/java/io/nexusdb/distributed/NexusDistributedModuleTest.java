package io.nexusdb.distributed;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusDistributedModuleTest {
    @Test
    void moduleNameMatches() {
        assertEquals("nexus-distributed", NexusDistributedModule.name());
    }
}

