package org.gene2life.execution;

import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobRun;
import org.gene2life.model.NodeProfile;
import org.gene2life.task.TaskResult;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

/**
 * Fault-tolerant task execution with retry mechanisms.
 * This addresses common workflow scheduling limitations around reliability.
 */
public final class FaultTolerantExecutor {
    
    private final int maxRetries;
    private final Duration baseDelay;
    private final boolean exponentialBackoff;
    private final BiFunction<Throwable, Integer, Boolean> retryPredicate;
    
    public FaultTolerantExecutor() {
        this(3, Duration.ofSeconds(1), true, 
             (exception, attempt) -> !(exception instanceof InterruptedException));
    }
    
    public FaultTolerantExecutor(int maxRetries, Duration baseDelay, 
                                 boolean exponentialBackoff,
                                 BiFunction<Throwable, Integer, Boolean> retryPredicate) {
        this.maxRetries = Math.max(0, maxRetries);
        this.baseDelay = baseDelay;
        this.exponentialBackoff = exponentialBackoff;
        this.retryPredicate = retryPredicate;
    }
    
    /**
     * Execute a task with fault tolerance (retry on failure).
     * 
     * @param job Job being executed
     * @param node Node executing the job
     * @param schedulerName Name of the scheduler
     * @param taskCallable The actual task execution
     * @return JobRun with success or failure information
     */
    public JobRun executeWithRetry(
            JobDefinition job,
            NodeProfile node,
            String schedulerName,
            Callable<TaskResult> taskCallable) {
        
        long overallStart = System.currentTimeMillis();
        Throwable lastException = null;
        int attempts = 0;
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            attempts++;
            long attemptStart = System.currentTimeMillis();
            
            try {
                TaskResult result = taskCallable.call();
                long finish = System.currentTimeMillis();
                long duration = Math.max(1L, finish - attemptStart);
                
                return new JobRun(
                        job.id(),
                        node.clusterId(),
                        node.nodeId(),
                        schedulerName,
                        0L, // predicted start - not known at execution time
                        0L, // predicted finish - not known at execution time
                        overallStart,
                        finish,
                        duration,
                        result.outputPath(),
                        result.description() + " (success after " + attempts + " attempt" + (attempts > 1 ? "s" : "") + ")");
                
            } catch (Exception exception) {
                lastException = exception;
                long attemptEnd = System.currentTimeMillis();
                
                // Check if we should retry
                if (attempt < maxRetries && shouldRetry(exception, attempt + 1)) {
                    long delayMs = calculateDelay(attempt);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return createFailureJobRun(job, node, schedulerName, overallStart, 
                                                   attemptEnd, ie, attempts);
                    }
                } else {
                    // No more retries
                    return createFailureJobRun(job, node, schedulerName, overallStart, 
                                               attemptEnd, exception, attempts);
                }
            }
        }
        
        // Should never reach here, but just in case
        return createFailureJobRun(job, node, schedulerName, overallStart, 
                                   System.currentTimeMillis(), lastException, attempts);
    }
    
    /**
     * Execute with automatic node failover - tries different nodes on failure.
     */
    public JobRun executeWithFailover(
            JobDefinition job,
            NodeProfile primaryNode,
            java.util.List<NodeProfile> failoverNodes,
            String schedulerName,
            java.util.function.Function<NodeProfile, Callable<TaskResult>> taskFactory) {
        
        long overallStart = System.currentTimeMillis();
        int totalAttempts = 0;
        
        // Try primary node first
        java.util.List<NodeProfile> allNodes = new java.util.ArrayList<>();
        allNodes.add(primaryNode);
        allNodes.addAll(failoverNodes);
        
        for (NodeProfile node : allNodes) {
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                totalAttempts++;
                long attemptStart = System.currentTimeMillis();
                
                try {
                    Callable<TaskResult> task = taskFactory.apply(node);
                    TaskResult result = task.call();
                    long finish = System.currentTimeMillis();
                    long duration = Math.max(1L, finish - attemptStart);
                    
                    String nodeInfo = node.nodeId().equals(primaryNode.nodeId()) ? "" : " (failover to " + node.nodeId() + ")";
                    return new JobRun(
                            job.id(),
                            node.clusterId(),
                            node.nodeId(),
                            schedulerName,
                            0L, 0L,
                            overallStart,
                            finish,
                            duration,
                            result.outputPath(),
                            result.description() + " (success after " + totalAttempts + 
                                " attempt" + (totalAttempts > 1 ? "s" : "") + nodeInfo + ")");
                    
                } catch (Exception exception) {
                    long attemptEnd = System.currentTimeMillis();
                    
                    if (attempt < maxRetries && shouldRetry(exception, attempt + 1)) {
                        long delayMs = calculateDelay(attempt);
                        try {
                            Thread.sleep(delayMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return createFailureJobRun(job, node, schedulerName, overallStart,
                                                       attemptEnd, ie, totalAttempts);
                        }
                    } else {
                        // Move to next node for failover
                        break;
                    }
                }
            }
        }
        
        // All nodes failed
        return createFailureJobRun(job, primaryNode, schedulerName, overallStart,
                                   System.currentTimeMillis(), 
                                   new RuntimeException("All nodes failed after " + totalAttempts + " attempts"),
                                   totalAttempts);
    }
    
    private boolean shouldRetry(Throwable exception, int attemptNumber) {
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }
        return retryPredicate.apply(exception, attemptNumber);
    }
    
    private long calculateDelay(int attempt) {
        long base = baseDelay.toMillis();
        if (!exponentialBackoff) {
            return base;
        }
        // Exponential backoff: base * 2^attempt, capped at 30 seconds
        long delay = base * (1L << attempt);
        return Math.min(delay, 30_000L);
    }
    
    private JobRun createFailureJobRun(
            JobDefinition job,
            NodeProfile node,
            String schedulerName,
            long startTime,
            long endTime,
            Throwable exception,
            int attempts) {
        
        return new JobRun(
                job.id(),
                node.clusterId(),
                node.nodeId(),
                schedulerName,
                0L, 0L,
                startTime,
                endTime,
                Math.max(1L, endTime - startTime),
                null, // No output path on failure
                "FAILED after " + attempts + " attempt" + (attempts > 1 ? "s" : "") + 
                    ": " + exception.getMessage());
    }
    
    // Getters for configuration
    public int maxRetries() { return maxRetries; }
    public Duration baseDelay() { return baseDelay; }
    public boolean exponentialBackoff() { return exponentialBackoff; }
}
