/**
 * Portfolio Project Part 2: Sequenced Dual-Thread Counter Application.
 *
 * Two worker threads share a counter. The first counts from 1 to 20; the
 * second waits for that phase to finish, then counts from 20 down to 0.
 * Both workers start before either is joined. A monitor controls the handoff.
 *
 * Compile: javac -Xlint:all CounterThreads.java
 * Run:     java CounterThreads
 */
public final class CounterThreads {
    private static final int COUNT_TARGET = 20;
    private static final int COUNT_FLOOR = 0;

    // Display pacing only. Synchronization does not depend on this delay.
    private static final long STEP_DELAY_MILLIS = 25L;

    private final Object stateLock = new Object();
    private final Object outputLock = new Object();

    // Every access to these mutable fields is guarded by stateLock.
    private int counter = COUNT_FLOOR;
    private boolean upPhaseDone = false;
    private Exception failure = null;

    private void printMessage(String message) {
        synchronized (outputLock) {
            System.out.println(message);
        }
    }

    private void printLine(String label, int value) {
        printMessage("[" + label + "] " + value);
    }

    private void printBanner(String message) {
        printMessage("\n=== " + message + " ===");
    }

    /** Wake any waiter if a worker is interrupted or encounters an error. */
    private void recordFailure(Exception cause) {
        synchronized (stateLock) {
            if (failure == null) {
                failure = cause;
            }
            stateLock.notifyAll();
        }
    }

    private void countUpTask(String label) {
        try {
            printBanner(label + " starting: counting up to " + COUNT_TARGET);

            for (int value = 1; value <= COUNT_TARGET; value++) {
                synchronized (stateLock) {
                    if (failure != null) {
                        return;
                    }
                    counter = value;
                }
                // Never hold stateLock while printing or sleeping.
                printLine(label, value);
                Thread.sleep(STEP_DELAY_MILLIS);
            }

            // Print before signaling so the phase banners stay in order.
            printBanner(label + " finished at " + COUNT_TARGET);
            synchronized (stateLock) {
                upPhaseDone = true;
                stateLock.notifyAll();
            }
        } catch (InterruptedException exception) {
            recordFailure(exception);
            Thread.currentThread().interrupt();
        } catch (RuntimeException exception) {
            recordFailure(exception);
        }
    }

    private void countDownTask(String label) {
        try {
            final int startValue;
            synchronized (stateLock) {
                // wait() releases the monitor and reacquires it before returning.
                // Recheck the condition to handle spurious wakeups and signals
                // that arrive before this worker begins waiting.
                while (!upPhaseDone && failure == null) {
                    stateLock.wait();
                }
                if (failure != null) {
                    return;
                }
                startValue = counter;
            }

            if (startValue != COUNT_TARGET) {
                throw new IllegalStateException("Synchronization error: expected "
                        + COUNT_TARGET + " but observed " + startValue);
            }

            printBanner(label + " starting: counting down to " + COUNT_FLOOR);
            // Java int is signed, so decrementing past zero ends this loop.
            // Fixed bounds keep all arithmetic within the int range.
            for (int value = startValue; value >= COUNT_FLOOR; value--) {
                synchronized (stateLock) {
                    if (failure != null) {
                        return;
                    }
                    counter = value;
                }
                printLine(label, value);
                Thread.sleep(STEP_DELAY_MILLIS);
            }

            printBanner(label + " finished at " + COUNT_FLOOR);
        } catch (InterruptedException exception) {
            recordFailure(exception);
            Thread.currentThread().interrupt();
        } catch (RuntimeException exception) {
            recordFailure(exception);
        }
    }

    private void run() throws InterruptedException {
        printMessage("Portfolio Project Part 2 - Concurrent Counter Demonstration");
        printMessage("Main thread: " + Thread.currentThread().getName());

        // Labels are immutable Strings. No user input or mutable text is shared.
        Thread upThread = new Thread(() -> countUpTask("Thread-1 UP"), "counter-up");
        Thread downThread = new Thread(() -> countDownTask("Thread-2 DOWN"), "counter-down");

        upThread.start();
        downThread.start();

        try {
            upThread.join();
            downThread.join();
        } catch (InterruptedException exception) {
            // Cancel both workers if the coordinating thread cannot keep waiting.
            recordFailure(exception);
            upThread.interrupt();
            downThread.interrupt();
            throw exception;
        }

        final int finalValue;
        synchronized (stateLock) {
            if (failure != null) {
                throw new IllegalStateException("Counter demonstration did not complete.", failure);
            }
            finalValue = counter;
        }
        if (finalValue != COUNT_FLOOR) {
            throw new IllegalStateException("Unexpected final counter value: " + finalValue);
        }
        printMessage("\nBoth threads joined. Final counter value: " + finalValue);
    }

    public static void main(String[] args) throws InterruptedException {
        new CounterThreads().run();
    }
}
