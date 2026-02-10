package org.example.functions.activity;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;

public class LogActivities {
    @FunctionName("Log")
    public void log(
            @DurableActivityTrigger(name = "msg") String msg,
            final ExecutionContext context) {

        context.getLogger().info(msg);
    }
}
