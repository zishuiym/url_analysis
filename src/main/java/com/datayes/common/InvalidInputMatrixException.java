package com.datayes.common;

@SuppressWarnings("serial")
public class InvalidInputMatrixException extends Exception {

	public InvalidInputMatrixException(String message) {
		super(message);
	}

	public InvalidInputMatrixException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidInputMatrixException(Throwable cause) {
		super(cause);
	}
}
