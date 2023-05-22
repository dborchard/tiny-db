package com.arjunsk.tiny_db.server.a_frontend.common.exception;

/**
 * A runtime exception indicating that the submitted query
 * has incorrect syntax.
 *
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
public class BadSyntaxException extends RuntimeException {
    public BadSyntaxException() {
    }
}
