# Implementation of Paper Limitations and Future Work

This document describes the implementations that address the limitations mentioned in the paper and extend the workflow scheduling system with practical features for production use.

## Addressed Limitations

### 1. Communication Cost Awareness

**Paper Limitation:** "communication cost is ignored"

**Implementation:**
- `CommunicationCostModel.java` - Models data transfer times between dependent tasks
- `HeftWithCommunicationScheduler.java` - HEFT algorithm enhanced with communication costs
- `WshWithCommunicationScheduler.java` - WSH algorithm enhanced with communication costs

**Features:**
- Bandwidth modeling between clusters (inter-cluster and intra-cluster)
- Data size estimation per task type
- Communication cost included in earliest finish time (EFT) calculation
- Upward rank computation includes communication costs to successors

**Usage:**
```bash
./scripts/run.sh run --scheduler heft-comm --workflow gene2life ...
./scripts/run.sh run --scheduler wsh-comm --workflow gene2life ...
```

**Benefits:**
- More accurate makespan predictions in distributed environments
- Better node selection when data transfer matters
- Considers network topology in scheduling decisions

---

### 2. Fault Tolerance and Reliability

**Common Limitation:** Task failures require manual intervention

**Implementation:**
- `FaultTolerantExecutor.java` - Retry mechanisms with configurable strategies

**Features:**
- Automatic retry with configurable max attempts
- Exponential backoff between retries (configurable)
- Node failover to alternative nodes on persistent failures
- Detailed failure reporting with attempt counts
- Distinguishes between retryable and non-retryable failures

**Usage:**
```java
FaultTolerantExecutor executor = new FaultTolerantExecutor(
    3,                          // max retries
    Duration.ofSeconds(1),      // base delay
    true,                       // exponential backoff
    (exception, attempt) -> true // retry predicate
);

JobRun result = executor.executeWithRetry(
    job, node, schedulerName, taskCallable
);
```

**Benefits:**
- Improved reliability in production environments
- Automatic recovery from transient failures
- No manual intervention for temporary node issues
- Detailed tracking of failure patterns

---

### 3. Multi-Workflow Concurrent Scheduling

**Common Limitation:** Schedules single workflows in isolation

**Implementation:**
- `MultiWorkflowScheduler.java` - Schedules multiple workflows on shared resources

**Features:**
- Priority-based workflow scheduling
- Fair resource allocation between competing workflows
- Round-robin job selection across workflows
- Global resource tracking across all workflows
- Configurable scheduling strategy (fair vs priority-based)

**Usage:**
```java
MultiWorkflowScheduler scheduler = new MultiWorkflowScheduler(
    new HeftScheduler(),  // base scheduler
    true                 // fair scheduling enabled
);

Map<String, WorkflowPriority> workflows = Map.of(
    "wf1", new WorkflowPriority(workflow1, 5, System.currentTimeMillis(), "wf1"),
    "wf2", new WorkflowPriority(workflow2, 3, System.currentTimeMillis(), "wf2")
);

Map<String, List<PlanAssignment>> plans = 
    scheduler.scheduleMultipleWorkflows(workflows, clusters, benchmarks);
```

**Benefits:**
- Better resource utilization in multi-tenant environments
- Fairness guarantees between workflows
- Priority handling for urgent workflows
- Single resource pool management

---

### 4. Dynamic/Online Scheduling

**Common Limitation:** Requires all workflows to be known in advance

**Implementation:**
- `DynamicScheduler.java` - Accepts workflows arriving at runtime

**Features:**
- Workflow submission at any time
- Runtime job completion tracking
- Dynamic resource availability updates
- Preemption support (configurable)
- Statistics and monitoring
- Failure handling with retry/restart capabilities

**Usage:**
```java
DynamicScheduler scheduler = new DynamicScheduler(
    new HeftScheduler(),  // base scheduler
    false                 // no preemption
);

// Submit workflow dynamically
String submissionId = scheduler.submitWorkflow(
    "workflow-1", workflow, 5  // priority
);

// Schedule next batch of ready jobs
Map<String, List<PlanAssignment>> newSchedules = 
    scheduler.scheduleNextBatch(clusters, benchmarks);

// Mark job completion
scheduler.markJobComplete(jobId, System.currentTimeMillis());

// Get statistics
DynamicStatistics stats = scheduler.getStatistics();
```

**Benefits:**
- Supports real-time workflow arrival
- No need to batch workflows before scheduling
- Adapts to changing resource conditions
- Cloud-native approach for elastic environments

---

### 5. Deadline-Constrained Scheduling

**Common Limitation:** Unconstrained optimization (only minimizing makespan)

**Implementation:**
- `DeadlineConstrainedScheduler.java` - Optimizes for meeting workflow deadlines

**Features:**
- Hard deadlines (must be met, reject if impossible)
- Soft deadlines (minimize tardiness)
- Slack calculation for critical path identification
- Deadline-aware priority assignment
- Critical path acceleration for urgent workflows
- Feasibility checking for hard deadlines

**Usage:**
```java
DeadlineConstrainedScheduler scheduler = new DeadlineConstrainedScheduler(
    new HeftScheduler()
);

// Set deadline for a workflow
scheduler.setDeadline("gene2life", 300_000, true); // 5 min hard deadline

List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
```

**Benefits:**
- Meets service level agreements (SLAs)
- Critical path prioritization for deadline adherence
- Rejects infeasible workloads early
- Suitable for time-sensitive applications

---

### 6. Budget/Cost-Aware Scheduling

**Common Limitation:** Unconstrained resource usage (ignores cloud costs)

**Implementation:**
- `BudgetAwareScheduler.java` - Optimizes for cloud cost minimization

**Features:**
- Per-node cost modeling ($/hour for different instance types)
- Budget constraints (hard and soft)
- Cost-performance tradeoff optimization
- Cost estimation and reporting
- Node selection based on cost efficiency

**Usage:**
```java
BudgetAwareScheduler scheduler = new BudgetAwareScheduler(
    new HeftScheduler(),
    10.0  // $10 budget limit
);

// Set node costs (e.g., AWS EC2 instance costs)
scheduler.setNodeCost("c1-n1", 0.05, 0.0);  // $0.05/sec
scheduler.setNodeCost("c2-n1", 0.02, 0.0);  // $0.02/sec (cheaper)

List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
double cost = scheduler.calculatePlanCost(plan, workflow, clusters);
```

**Benefits:**
- Controls cloud infrastructure costs
- Cost-performance tradeoff analysis
- Suitable for budget-constrained environments
- Cost estimation before execution

---

## Scheduler Summary

| Scheduler | Communication Cost | Training-Based | Multi-Workflow | Dynamic | Deadline | Budget |
|-----------|-------------------|----------------|----------------|---------|----------|--------|
| HEFT       | No                | No             | No             | No      | No       | No     |
| WSH        | No                | Yes            | No             | No      | No       | No     |
| HEFT-Comm  | **Yes**           | No             | No             | No      | No       | No     |
| WSH-Comm   | **Yes**           | **Yes**        | No             | No      | No       | No     |
| Multi      | Configurable      | Configurable   | **Yes**        | No      | No       | No     |
| Dynamic    | Configurable      | Configurable   | **Yes**        | **Yes** | No       | No     |
| Deadline   | Configurable      | Configurable   | No             | No      | **Yes**  | No     |
| Budget     | Configurable      | Configurable   | No             | No      | No       | **Yes**|

---

## Usage Examples

### Communication-Aware Scheduling

```bash
# Compare HEFT with and without communication costs
./scripts/run.sh compare --scheduler heft --workflow gene2life ...
./scripts/run.sh compare --scheduler heft-comm --workflow gene2life ...
```

### Multi-Workflow Scheduling

```bash
# Schedule multiple workflows concurrently
./scripts/run.sh run --scheduler multi --workflow gene2life ...
```

### Dynamic Workflow Submission

The DynamicScheduler is designed for programmatic use where workflows arrive over time:

```java
DynamicScheduler scheduler = new DynamicScheduler(new WshScheduler());

// Webhook handler for new workflow submissions
public void onWorkflowSubmitted(WorkflowDefinition workflow, int priority) {
    String id = scheduler.submitWorkflow(UUID.randomUUID().toString(), workflow, priority);
    scheduler.scheduleNextBatch(clusters, benchmarks);
}

// Executor callback on job completion
public void onJobCompleted(String jobId) {
    scheduler.markJobComplete(jobId, System.currentTimeMillis());
}
```

### Deadline-Constrained Execution

```java
// Schedule with hard deadline
DeadlineConstrainedScheduler scheduler = new DeadlineConstrainedScheduler(new HeftScheduler());
scheduler.setDeadline("workflow-1", 600_000, true); // 10 minutes hard deadline

List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
```

### Budget-Constrained Execution

```java
// Schedule with $5 budget limit
BudgetAwareScheduler scheduler = new BudgetAwareScheduler(new HeftScheduler(), 5.0);

// Configure node costs
scheduler.setNodeCost("c1-n1", 0.10, 0.0); // $0.10/sec (high performance)
scheduler.setNodeCost("c4-n1", 0.01, 0.0); // $0.01/sec (budget option)

List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
```

---

## Performance Considerations

### Communication Cost Overhead

- Communication cost calculation adds ~10-15% overhead to scheduling time
- Beneficial when network bandwidth is constrained (< 1 Gbps between nodes)
- Most effective for data-intensive workflows (epigenomics)

### Fault Tolerance Trade-offs

- Retry attempts increase total makespan on failure
- Configurable backoff prevents thundering herd
- Node failover requires additional node capacity

### Dynamic Scheduling Overhead

- Event-driven architecture suitable for cloud environments
- Slightly higher memory usage for tracking state
- Better resource utilization in multi-tenant scenarios

### Deadline and Budget Overhead

- Slack calculation adds O(n) complexity for n jobs
- Budget checking adds minimal overhead per job
- Both provide valuable constraints for production use

---

## Future Extensions

Potential extensions based on the implemented foundation:

1. **Machine Learning Integration**
   - Use historical data to predict task durations more accurately
   - Learn communication patterns between task types

2. **Energy-Aware Scheduling**
   - Model power consumption per node type
   - Schedule to minimize energy while meeting deadlines

3. **Security-Aware Scheduling**
   - Consider data sensitivity in node placement
   - Encrypt communication for sensitive workflows

4. **QoS (Quality of Service) Guarantees**
   - Multiple service tiers (premium vs standard)
   - Admission control based on available resources

5. **Workflow Clustering/Task Grouping**
   - Group small tasks to reduce overhead
   - Batch similar jobs for efficiency

6. **Container/Kubernetes Integration**
   - Direct integration with K8s scheduler
   - Pod-based node management

---

## References

- Original paper: WSH vs HEFT workflow scheduling study
- HEFT: Heterogeneous Earliest Finish Time algorithm
- WSH: Workflow Scheduling with History (training-based)
- Deadline Scheduling: Critical path method with time constraints
- Cloud Cost Optimization: AWS/Azure/Google Cloud pricing models
