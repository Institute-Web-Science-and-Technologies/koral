package de.uni_koblenz.west.cidre.common.config;

public class SerializationException extends RuntimeException {

	private static final long serialVersionUID = -3885880097620883301L;

	public SerializationException() {
		super();
	}

	public SerializationException(String message, Throwable cause) {
		super(message, cause);
	}

	public SerializationException(String message) {
		super(message);
	}

	public SerializationException(Throwable cause) {
		super(cause);
	}

}
