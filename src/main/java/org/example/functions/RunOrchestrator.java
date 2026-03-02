package org.example.functions;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.sql.*;
import java.time.Duration;
import java.util.*;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;
import org.example.functions.activity.UserActivities;
import org.example.functions.model.ActivityResult;

/**
 * Please follow the below steps to run this durable function sample
 * 1. Send an HTTP GET/POST request to endpoint `StartHelloCities` to run a durable function
 * 2. Send request to statusQueryGetUri in `StartHelloCities` response to get the status of durable function
 * For more instructions, please refer https://aka.ms/durable-function-java
 * 
 * Please add com.microsoft:durabletask-azure-functions to your project dependencies
 * Please add `"extensions": { "durableTask": { "hubName": "JavaTestHub" }}` to your host.json
 */
public class RunOrchestrator {
    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("StartOrchestration")
    public HttpResponseMessage startOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("Cities");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * This is the orchestrator function, which can schedule activity functions, create durable timers,
     * or wait for external events in a way that's completely fault-tolerant.
     */
    @FunctionName("Cities")
    public String citiesOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String result = "";

        UserActivities.RegisterUserRequest req = new UserActivities.RegisterUserRequest();
        req.firstName = "Ana";
        req.lastName = "Pérez";
        req.email = "ana3@correo.com";

        try {
            RetryPolicy retryPolicy = new RetryPolicy(
                    3,
                    Duration.ofSeconds(2)
            ).setBackoffCoefficient(1.0);

            TaskOptions options = new TaskOptions(retryPolicy);

            result += ctx.callActivity("Capitalize", "Austin",options, String.class).await() + ", ";
             ctx.callActivity("Validate", req , ActivityResult.class).await() ;
            ActivityResult  r = ctx.callActivity("RegisterUser", req, ActivityResult.class).await();

            if (!r.success && "DUPLICATE".equals(r.code)) {
                throw new DuplicateEmailException(r.message);
            }

           
        return result;
    } catch (DuplicateEmailException ex) {
            ctx.callActivity("Log", ex.getMessage() + " taskName: Duplicate"  , Void.class).await();
            throw ex;
        }
        catch (TaskFailedException ex) {
            ctx.callActivity("Log", ex.getMessage() + " taskName:" + ex.getTaskName() , Void.class).await();
        throw ex;
    }
    }

    /**
     * This is the activity function that gets invoked by the orchestration.
     */
    @FunctionName("Capitalize")
    public String capitalize(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        context.getLogger().info("Capitalizing: " + name);
        //throw new ArithmeticException("error artimetico");
        return name.toUpperCase();
    }
    @FunctionName("Validate")
    public ActivityResult  validate(
            @DurableActivityTrigger(name = "req") UserActivities.RegisterUserRequest  req,
            final ExecutionContext context) {

        /*String raw = email;
        String normalized = email == null ? "" : email.replace("\"", "").trim();
         */
        String email = req.email;

        context.getLogger().info("Validate: email=" + email);

        final String sql = "SELECT * FROM [dbo].[Users] WHERE Email = ?";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, email);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    context.getLogger().info("Validate: ya existe (DUPLICATE) email=" + email);
                    return ActivityResult.fail("DUPLICATE", "Ya procesado: " + email);
                }
            }

            context.getLogger().info("Validate: no existe (OK) email=" + email);
            return ActivityResult.ok("No existe, continuar: " + email);

        } catch (SQLException e) {
            context.getLogger().severe("Validate DB error email=" + email + " err=" + e.getMessage());

            // Opción A (recomendada): lanzar excepción => Durable retry (si configuraste RetryPolicy)
            throw new RuntimeException("DB_ERROR en Validate: " + e.getMessage(), e);

            // Opción B (si NO quieres retry):
            // return ActivityResult.fail("DB_ERROR", "DB error en Validate: " + e.getMessage());
        }
    }

    private Connection getConnection() throws SQLException {
        // Usa una App Setting / local.settings.json:
        // "SQL_CONNECTION_STRING": "jdbc:sqlserver://...;databaseName=...;user=...;password=...;encrypt=true;"
        String url = System.getenv("SQL_JDBC_URL");
        if (url == null || url.isBlank()) {
            throw new SQLException("Falta SQL_CONNECTION_STRING en variables de entorno");
        }
        return DriverManager.getConnection(url);
    }
}