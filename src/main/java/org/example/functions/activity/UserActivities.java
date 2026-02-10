package org.example.functions.activity;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import org.example.functions.model.ActivityResult;

import java.sql.*;

public class UserActivities {

    public static class RegisterUserRequest {
        public String firstName;
        public String lastName;
        public String email;
    }



    @FunctionName("RegisterUser")
    public ActivityResult registerUser(
            @DurableActivityTrigger(name = "input") RegisterUserRequest input,
            final ExecutionContext context) {

        try {


            String email = input.email.trim().toLowerCase();

            // App Settings / Environment variables
            String jdbcUrl = System.getenv("SQL_JDBC_URL");   // ej: jdbc:sqlserver://localhost:1433;databaseName=MiDB;encrypt=true;trustServerCertificate=true
            //String dbUser  = System.getenv("DB_USER");
            //String dbPass  = System.getenv("DB_PASSWORD");

            if (jdbcUrl == null || jdbcUrl.isBlank()) throw new IllegalStateException("DB_JDBC_URL no configurado");
            //if (dbUser  == null || dbUser.isBlank())  throw new IllegalStateException("DB_USER no configurado");
            //if (dbPass  == null || dbPass.isBlank())  throw new IllegalStateException("DB_PASSWORD no configurado");

            // INSERT + retorno del ID
            String sql = """
                INSERT INTO dbo.Users (FirstName, LastName, Email)
                OUTPUT INSERTED.Id
                VALUES (?, ?, ?);
                """;

            try (Connection conn = DriverManager.getConnection (jdbcUrl);
                 PreparedStatement ps = conn.prepareStatement(sql)) {

                ps.setString(1, input.firstName.trim());
                ps.setString(2, input.lastName.trim());
                ps.setString(3, email);

                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    long id = rs.getLong(1);
                    context.getLogger().info("Usuario creado id=" + id + " email=" + email);
                    return ActivityResult.ok("Usuario registrado (id=" + id + ")");
                }
            }

        } catch (SQLException ex) {
            // SQL Server: 2627 = Violation of PRIMARY KEY/UNIQUE constraint
            //            2601 = Cannot insert duplicate key row in object with unique index
            int code = ex.getErrorCode();
            if (code == 2627 || code == 2601) {
                context.getLogger().warning("Email duplicado: " + ex.getMessage());
                return ActivityResult.fail("DUPLICATE", "El correo ya está registrado");
            }

            context.getLogger().severe("SQL error (" + code + "): " + ex.getMessage());
            throw new RuntimeException( ex);

        } catch (Exception ex) {

            context.getLogger().severe("Error registrando usuario: " + ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
