package edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr;

/**
 * A runtime exception indicating that the transaction
 * needs to abort because a buffer request could not be satisfied.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
public class BufferAbortException extends RuntimeException {}
