package org.example.functions.model;

public class ActivityResult {
    public boolean success;
    public String code;     // OK, DUPLICATE, VALIDATION, DB_ERROR, UNEXPECTED
    public String message;

    public static ActivityResult ok(String msg) {
        ActivityResult r = new ActivityResult();
        r.success = true;
        r.code = "OK";
        r.message = msg;
        return r;
    }

    public static ActivityResult fail(String code, String msg) {
        ActivityResult r = new ActivityResult();
        r.success = false;
        r.code = code;
        r.message = msg;
        return r;
    }
}