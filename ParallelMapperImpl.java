import java.util.*;
import java.util.function.Function;

/**
 * @author Bocharnikov
 * @version 1.0.0
 */

public class ParallelMapperImpl implements ParallelMapper {

    private final List<Thread> threads;
    private final TasksQueue tasks;
    private volatile Boolean closed;

    /**
     * Thread-count constructor.
     * Creates a ParallelMapperImpl instance operating with maximum of {@code threadsNumber}
     * threads of type {@link Thread}.
     *
     * @param threadsNumber maximum count of operable threads
     */
    public ParallelMapperImpl(final int threadsNumber) {
        tasks = new TasksQueue();
        closed = false;
        final Runnable baseTask = () -> {
            try {
                while (!Thread.interrupted()) {
                    tasks.getTask().run();
                }
            } catch (final InterruptedException ignored) {
            } finally {
                Thread.currentThread().interrupt();
            }
        };

        threads = new ArrayList<>();
        for (int i = 0; i < threadsNumber; i++) {
            threads.add(new Thread(baseTask));
        }
        threads.forEach(Thread::start);
    }

    private static class TasksQueue {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        public synchronized Runnable getTask() throws InterruptedException {
            while (tasks.isEmpty()) {
                wait();
            }
            return tasks.poll();
        }

        public synchronized void addTask(final Runnable task) {
            tasks.add(task);
            notify();
        }
    }

    private static class TaskWrapper<T> {
        final List<T> result;
        RuntimeException exception;
        int resultDone;
        private volatile Boolean closed;

        TaskWrapper(final int size, Boolean closeLink) {
            result = new ArrayList<>(Collections.nCopies(size, null));
            closed = closeLink;
        }

        public synchronized void setResult(final int pos, final T value) {
            result.set(pos, value);
            checkDone();
        }

        public synchronized void addException(final RuntimeException e) {
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
            checkDone();
        }

        private void checkDone() {
            resultDone++;
            if (resultDone == result.size()) {
                notify();
            }
        }

        public synchronized List<T> getResult() throws InterruptedException {
            while (resultDone != result.size() && !closed) {
                wait();
            }
            if (closed) {
                throw new InterruptedException("Some maps were running, but close was called");
            }
            if (exception != null) {
                throw exception;
            }
            return result;
        }
    }

    /**
     * Maps function {@code f} over specified {@code args}.
     * Mapping for each element performs in parallel.
     *
     * @throws InterruptedException if calling thread was interrupted
     */
    @Override
    public <T, R> List<R> map(final Function<? super T, ? extends R> f, final List<? extends T> args) throws InterruptedException {
        if (closed) {
            throw new InterruptedException("ParallelMapper is closed");
        }

        final TaskWrapper<R> task = new TaskWrapper<>(args.size(), closed);
        int index = 0;
        for (final T arg : args) {
            final int indexFinal = index++;
            tasks.addTask(() -> {
                try {
                    task.setResult(indexFinal, f.apply(arg));
                } catch (final RuntimeException e) {
                    task.addException(e);
                }
            });
        }
        return task.getResult();
    }

    /** Stops all threads. All unfinished mappings leave in undefined state. */
    @Override
    public synchronized void close() {
        threads.forEach(Thread::interrupt);
        closed = true;
        notifyAll();
        for (final Thread thread : threads) {
            while (true) {
                try {
                    thread.join();
                    break;
                } catch (final InterruptedException ignored) {
                }
            }
        }
    }
}

