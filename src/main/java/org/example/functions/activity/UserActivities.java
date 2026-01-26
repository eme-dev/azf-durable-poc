package org.example.functions.activity;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;

import java.sql.*;

public class UserActivities {

    public static class RegisterUserRequest {
        public String firstName;
        public String lastName;
        public String email;
    }

    public static class RegisterUserResult {
        public boolean success;
        public String message;
        public Long userId;

        public static RegisterUserResult ok(long id) {
            RegisterUserResult r = new RegisterUserResult();
            r.success = true;
            r.userId = id;
            r.message = "Usuario registrado";
            return r;
        }

        public static RegisterUserResult fail(String msg) {
            RegisterUserResult r = new RegisterUserResult();
            r.success = false;
            r.message = msg;
            return r;
        }
    }

    @FunctionName("RegisterUser")
    public RegisterUserResult registerUser(
            @DurableActivityTrigger(name = "input") RegisterUserRequest input,
            final ExecutionContext context) {

        try {
            // Validaciones básicas
            if (input == null) return RegisterUserResult.fail("Input es null");
            if (isBlank(input.firstName)) return RegisterUserResult.fail("firstName es requerido");
            if (isBlank(input.lastName))  return RegisterUserResult.fail("lastName es requerido");
            if (isBlank(input.email))     return RegisterUserResult.fail("email es requerido");

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
                    return RegisterUserResult.ok(id);
                }
            }

        } catch (SQLException ex) {
            // SQL Server: 2627 = Violation of PRIMARY KEY/UNIQUE constraint
            //            2601 = Cannot insert duplicate key row in object with unique index
            int code = ex.getErrorCode();
            if (code == 2627 || code == 2601) {
                context.getLogger().warning("Email duplicado: " + ex.getMessage());
                return RegisterUserResult.fail("El correo ya está registrado");
            }

            context.getLogger().severe("SQL error (" + code + "): " + ex.getMessage());
            return RegisterUserResult.fail("Error de base de datos");

        } catch (Exception ex) {

            context.getLogger().severe("Error registrando usuario: " + ex.getMessage());
            return RegisterUserResult.fail("Error interno");
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
