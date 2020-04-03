import ParallelMapper;

import java.util.*;
import java.util.function.Function;

/**
 * @author Bocharnikov
 * @version 1.0.0
 */

public class ParallelMapperImpl implements ParallelMapper {

    private final List<Thread> threads;
    private final TasksQueue tasks;
    private List<Boolean> interuptMaps;
    private List<Object> interupt;
    volatile boolean closed;
    final Integer synchronize;

    /**
     * Thread-count constructor.
     * Creates a ParallelMapperImpl instance operating with maximum of {@code threadsNumber}
     * threads of type {@link Thread}.
     *
     * @param threadsNumber maximum count of operable threads
     */
    public ParallelMapperImpl(final int threadsNumber) {
        synchronize = 42;
        tasks = new TasksQueue();
        interuptMaps = new ArrayList<>();
        interupt = new ArrayList<>();
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
        volatile Boolean closed;

        TaskWrapper(final int size) {
            closed = false;
            result = new ArrayList<>(Collections.nCopies(size, null));
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
        final TaskWrapper<R> task = new TaskWrapper<>(args.size());
        int id = 0;
        for (final T arg : args) {
            final int index = id;
            final Runnable newTask = () -> {
                try {
                    task.setResult(index, f.apply(arg));
                } catch (final RuntimeException e) {
                    task.addException(e);
                }
            };
            id++;
            tasks.addTask(newTask);
        }
        synchronized (synchronize) { // case чтобы старые мапы уже убиты и добавляются эти, они будут вечно wait'ить
            if (!closed) {
                interuptMaps.add(task.closed);
                interupt.add(task);
            }
        }
        return task.getResult();
    }

    /** Stops all threads. All unfinished mappings leave in undefined state. */
    @Override
    public void close() {
        threads.forEach(Thread::interrupt);
        synchronized (synchronize) {
            closed = true;
            for (int i = 0; i < interuptMaps.size(); i++) {
                interuptMaps.set(i, true);
            }
            for (Object inter : interupt) {
                synchronized (inter) {
                    inter.notify();
                }
            }
        }
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
