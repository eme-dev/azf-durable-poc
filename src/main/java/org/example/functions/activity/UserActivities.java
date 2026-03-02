package org.example.functions.activity;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import org.example.functions.config.TransactionManager;
import org.example.functions.model.ActivityResult;

import java.sql.*;

public class UserActivities {

    private static final TransactionManager txManager = new TransactionManager();
    public static class RegisterUserRequest {
        public String firstName;
        public String lastName;
        public String email;
    }



    @FunctionName("RegisterUser")
    public ActivityResult registerUser(
            @DurableActivityTrigger(name = "input") RegisterUserRequest input,
            final ExecutionContext context) {

        try (TransactionManager tx = new TransactionManager().begin()) {

            Connection conn = tx.getConnection();
            String sql = "INSERT INTO dbo.Users (FirstName, LastName, Email) OUTPUT INSERTED.Id VALUES (?, ?, ?)";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, input.firstName);
                ps.setString(2, input.lastName);
                ps.setString(3, input.email.toLowerCase().trim());
                int i=2;
                int j;


                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    long id = rs.getLong(1);
                    //j=10/(i-2);
                    tx.commit();
                    return ActivityResult.ok("ID: " + id);
                }
            }

        } catch (SQLException ex) {
            // Manejo de duplicados (2627/2601) sigue igual...
            if (ex.getErrorCode() == 2627 || ex.getErrorCode() == 2601) {
                return ActivityResult.fail("DUPLICATE", "Email ya existe");
            }
            throw new RuntimeException("Error en DB", ex);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
