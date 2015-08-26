package de.uni_koblenz.west.cidre.common.config;

public class DeserializationException extends RuntimeException {

	private static final long serialVersionUID = 7961496401353020402L;

	public DeserializationException() {
	}

	public DeserializationException(String message) {
		super(message);
	}

	public DeserializationException(Throwable cause) {
		super(cause);
	}

	public DeserializationException(String message, Throwable cause) {
		super(message, cause);
	}

}
