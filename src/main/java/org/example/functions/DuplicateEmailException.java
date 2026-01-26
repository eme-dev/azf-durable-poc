package org.example.functions;

public class DuplicateEmailException extends RuntimeException {
    public DuplicateEmailException(String email) {
        super("Correo duplicado: " + email);
    }
}
