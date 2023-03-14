package com.tong.kafka.common;

import java.util.concurrent.ScheduledFuture;

public interface AdapterScheduler {
    /**
     * Initialize this scheduler so it is ready to accept scheduling of tasks
     */
    void startup();

    /**
     * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur.
     * This includes tasks scheduled with a delayed execution.
     */
    void shutdown();

    boolean isStarted();

    /**
     * Schedule a task
     *
     * @param name   The name of this task
     * @param delay  The amount of time to wait before the first execution
     * @param period The period with which to execute the task. If < 0 the task will execute only once.
     * @return A Future object to manage the task scheduled.
     */
    ScheduledFuture<?> schedule(String name, Runnable runnable, Integer delay, Integer period);

    ScheduledFuture<?> scheduleOnce(String name, Runnable command, Integer delay);
}
