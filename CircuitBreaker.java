public class CircuitBreaker {
    enum State { CLOSED, OPEN, HALF_OPEN }

    private State state = State.CLOSED;
    private int failureCount = 0;
    private final int failureThreshold = 3;
    private final long retryTimePeriod = 5000; // 5 seconds
    private long lastFailureTime = 0;

    public boolean allowRequest() {
        long currentTime = System.currentTimeMillis();
        
        if (state == State.OPEN) {
            if ((currentTime - lastFailureTime) > retryTimePeriod) {
                state = State.HALF_OPEN;
                return true;
            }
            return false;
        }
        return true;
    }

    public void recordSuccess() {
        failureCount = 0;
        state = State.CLOSED;
    }

    public void recordFailure() {
        failureCount++;
        lastFailureTime = System.currentTimeMillis();

        if (failureCount >= failureThreshold) {
            state = State.OPEN;
        }
    }

    public State getState() {
        return state;
    }
}
