package ch.ehi.openfgdb4j;

public class OpenFgdbException extends Exception {
    private final int errorCode;

    public OpenFgdbException(String message) {
        this(message, -1);
    }

    public OpenFgdbException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public OpenFgdbException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = -1;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
