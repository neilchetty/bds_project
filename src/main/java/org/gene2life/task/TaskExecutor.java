package org.gene2life.task;

import org.gene2life.model.NodeProfile;

public interface TaskExecutor {
    TaskResult execute(TaskInputs inputs, NodeProfile nodeProfile) throws Exception;
}
