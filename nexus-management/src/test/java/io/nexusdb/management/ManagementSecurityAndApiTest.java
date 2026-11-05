package io.nexusdb.management;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class ManagementSecurityAndApiTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private String loginAndGetToken(String username, String password) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}"))
                .andExpect(status().isOk())
                .andReturn();

        JsonNode node = objectMapper.readTree(result.getResponse().getContentAsString());
        return node.get("token").asText();
    }

    @Test
    @DisplayName("Login with valid and invalid credentials")
    void testAuthentication() throws Exception {
        // Valid admin
        mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"admin\",\"password\":\"admin123\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").isNotEmpty())
                .andExpect(jsonPath("$.role").value("ADMIN"));

        // Invalid credentials
        mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"admin\",\"password\":\"wrongpw\"}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("Unauthenticated request to protected endpoint is rejected")
    void testUnauthenticatedAccessRejected() throws Exception {
        mockMvc.perform(get("/api/cluster/topology"))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("VIEWER role can read topology, schema, metrics, but cannot execute faults or DML")
    void testViewerRolePermissions() throws Exception {
        String viewerToken = loginAndGetToken("viewer", "viewer123");

        // Allowed: topology & schema
        mockMvc.perform(get("/api/cluster/topology")
                        .header("Authorization", "Bearer " + viewerToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());

        mockMvc.perform(get("/api/schema")
                        .header("Authorization", "Bearer " + viewerToken))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/metrics/timeseries")
                        .header("Authorization", "Bearer " + viewerToken))
                .andExpect(status().isOk());

        // Denied: fault action (requires ADMIN)
        mockMvc.perform(post("/api/faults/action")
                        .header("Authorization", "Bearer " + viewerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"targetNode\":\"node-1\",\"action\":\"STOP\",\"durationMs\":1000}"))
                .andExpect(status().isForbidden());

        // Denied: SQL execute (requires ADMIN or DEVELOPER)
        mockMvc.perform(post("/api/sql/execute")
                        .header("Authorization", "Bearer " + viewerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sql\":\"SELECT * FROM users;\"}"))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("ADMIN role can trigger fault action and view audit log")
    void testAdminFaultActionAndAuditLog() throws Exception {
        String adminToken = loginAndGetToken("admin", "admin123");

        // ADMIN can trigger fault
        mockMvc.perform(post("/api/faults/action")
                        .header("Authorization", "Bearer " + adminToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"targetNode\":\"node-1\",\"action\":\"STOP\",\"durationMs\":2000}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));

        // Audit log reflects the action
        mockMvc.perform(get("/api/faults/audit-log")
                        .header("Authorization", "Bearer " + adminToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].username").value("admin"))
                .andExpect(jsonPath("$[0].action").value("FAULT_INJECTION"))
                .andExpect(jsonPath("$[0].target_node").value("node-1"));
    }

    @Test
    @DisplayName("DEVELOPER role can execute SQL and view query history")
    void testDeveloperSqlExecutionAndHistory() throws Exception {
        String devToken = loginAndGetToken("dev", "dev123");

        // DEVELOPER can execute SQL
        mockMvc.perform(post("/api/sql/execute")
                        .header("Authorization", "Bearer " + devToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sql\":\"SELECT * FROM users;\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rows").isArray());

        // Query appears in history
        mockMvc.perform(get("/api/sql/history")
                        .header("Authorization", "Bearer " + devToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].username").value("dev"))
                .andExpect(jsonPath("$[0].sql_query").value("SELECT * FROM users;"));

        // DEVELOPER can run EXPLAIN
        mockMvc.perform(post("/api/sql/explain")
                        .header("Authorization", "Bearer " + devToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sql\":\"SELECT * FROM users;\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.explanation").isNotEmpty());
    }

    @Test
    @DisplayName("Prometheus metrics endpoint is publicly available")
    void testPrometheusEndpoint() throws Exception {
        MvcResult result = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        assertThat(body).contains("nexus_cluster_cache_hit_ratio");
    }
}
