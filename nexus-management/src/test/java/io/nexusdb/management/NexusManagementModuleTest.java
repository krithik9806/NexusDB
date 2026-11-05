package io.nexusdb.management;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NexusManagementModuleTest {
    @Test
    void moduleNameMatches() {
        assertEquals("nexus-management", NexusManagementModule.name());
    }
}

