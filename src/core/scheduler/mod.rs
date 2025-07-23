// src/core/scheduler/mod.rs

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Task priority levels for the scheduler
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

/// Task status in the scheduler
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

/// A scheduled task
#[derive(Debug, Clone)]
pub struct Task {
    pub id: u64,
    pub name: String,
    pub priority: Priority,
    pub status: TaskStatus,
    pub created_at: Instant,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
    pub execution_duration: Option<Duration>,
}

/// Comprehensive scheduler statistics
#[derive(Debug, Clone, Default)]
pub struct SchedulerStats {
    pub total_tasks: u64,
    pub pending_tasks: u64,
    pub running_tasks: u64,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub cancelled_tasks: u64,
    pub average_execution_time: Duration,
    pub total_execution_time: Duration,
    pub tasks_by_priority: HashMap<Priority, u64>,
    pub uptime: Duration,
    pub last_task_completed: Option<Instant>,
}

/// Main scheduler implementation
pub struct Scheduler {
    tasks: Arc<Mutex<HashMap<u64, Task>>>,
    next_task_id: Arc<Mutex<u64>>,
    start_time: Instant,
}

impl Scheduler {
    /// Create a new scheduler instance
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            next_task_id: Arc::new(Mutex::new(1)),
            start_time: Instant::now(),
        }
    }

    /// Schedule a new task
    pub fn schedule_task(&self, name: String, priority: Priority) -> Result<u64, String> {
        let mut next_id = self.next_task_id.lock().map_err(|e| format!("Lock error: {}", e))?;
        let task_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let task = Task {
            id: task_id,
            name,
            priority,
            status: TaskStatus::Pending,
            created_at: Instant::now(),
            started_at: None,
            completed_at: None,
            execution_duration: None,
        };

        let mut tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        tasks.insert(task_id, task);

        Ok(task_id)
    }

    /// Start executing a task
    pub fn start_task(&self, task_id: u64) -> Result<(), String> {
        let mut tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        if let Some(task) = tasks.get_mut(&task_id) {
            if task.status == TaskStatus::Pending {
                task.status = TaskStatus::Running;
                task.started_at = Some(Instant::now());
                Ok(())
            } else {
                Err(format!("Task {} is not in pending state", task_id))
            }
        } else {
            Err(format!("Task {} not found", task_id))
        }
    }

    /// Complete a task successfully
    pub fn complete_task(&self, task_id: u64) -> Result<(), String> {
        let mut tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        if let Some(task) = tasks.get_mut(&task_id) {
            if task.status == TaskStatus::Running {
                let now = Instant::now();
                task.status = TaskStatus::Completed;
                task.completed_at = Some(now);
                
                if let Some(started_at) = task.started_at {
                    task.execution_duration = Some(now - started_at);
                }
                
                Ok(())
            } else {
                Err(format!("Task {} is not in running state", task_id))
            }
        } else {
            Err(format!("Task {} not found", task_id))
        }
    }

    /// Fail a task with an error message
    pub fn fail_task(&self, task_id: u64, error: String) -> Result<(), String> {
        let mut tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        if let Some(task) = tasks.get_mut(&task_id) {
            let now = Instant::now();
            task.status = TaskStatus::Failed(error);
            task.completed_at = Some(now);
            
            if let Some(started_at) = task.started_at {
                task.execution_duration = Some(now - started_at);
            }
            
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id))
        }
    }

    /// Cancel a pending task
    pub fn cancel_task(&self, task_id: u64) -> Result<(), String> {
        let mut tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        if let Some(task) = tasks.get_mut(&task_id) {
            if task.status == TaskStatus::Pending {
                task.status = TaskStatus::Cancelled;
                task.completed_at = Some(Instant::now());
                Ok(())
            } else {
                Err(format!("Task {} cannot be cancelled in current state", task_id))
            }
        } else {
            Err(format!("Task {} not found", task_id))
        }
    }

    /// Get comprehensive scheduler statistics
    /// 
    /// This method provides detailed statistics about the scheduler's performance,
    /// task distribution, and operational metrics. Unlike a placeholder implementation,
    /// this returns actual computed statistics from the scheduler's state.
    pub fn get_stats(&self) -> Result<SchedulerStats, String> {
        let tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        let mut stats = SchedulerStats {
            uptime: self.start_time.elapsed(),
            ..Default::default()
        };

        let mut total_execution_time = Duration::new(0, 0);
        let mut completed_count = 0;
        let mut last_completed: Option<Instant> = None;

        // Analyze all tasks
        for task in tasks.values() {
            stats.total_tasks += 1;

            // Count by status
            match &task.status {
                TaskStatus::Pending => stats.pending_tasks += 1,
                TaskStatus::Running => stats.running_tasks += 1,
                TaskStatus::Completed => {
                    stats.completed_tasks += 1;
                    completed_count += 1;
                    
                    // Track execution time
                    if let Some(duration) = task.execution_duration {
                        total_execution_time += duration;
                    }
                    
                    // Track last completion time
                    if let Some(completed_at) = task.completed_at {
                        if last_completed.is_none() || completed_at > last_completed.unwrap() {
                            last_completed = Some(completed_at);
                        }
                    }
                }
                TaskStatus::Failed(_) => stats.failed_tasks += 1,
                TaskStatus::Cancelled => stats.cancelled_tasks += 1,
            }

            // Count by priority
            *stats.tasks_by_priority.entry(task.priority).or_insert(0) += 1;
        }

        // Calculate average execution time
        if completed_count > 0 {
            stats.average_execution_time = total_execution_time / completed_count as u32;
        }
        
        stats.total_execution_time = total_execution_time;
        stats.last_task_completed = last_completed;

        Ok(stats)
    }

    /// Get a specific task by ID
    pub fn get_task(&self, task_id: u64) -> Result<Option<Task>, String> {
        let tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        Ok(tasks.get(&task_id).cloned())
    }

    /// Get all tasks with a specific status
    pub fn get_tasks_by_status(&self, status: TaskStatus) -> Result<Vec<Task>, String> {
        let tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        let filtered_tasks: Vec<Task> = tasks
            .values()
            .filter(|task| std::mem::discriminant(&task.status) == std::mem::discriminant(&status))
            .cloned()
            .collect();
            
        Ok(filtered_tasks)
    }

    /// Get all tasks with a specific priority
    pub fn get_tasks_by_priority(&self, priority: Priority) -> Result<Vec<Task>, String> {
        let tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        
        let filtered_tasks: Vec<Task> = tasks
            .values()
            .filter(|task| task.priority == priority)
            .cloned()
            .collect();
            
        Ok(filtered_tasks)
    }

    /// Clear completed and failed tasks older than the specified duration
    pub fn cleanup_old_tasks(&self, older_than: Duration) -> Result<u64, String> {
        let mut tasks = self.tasks.lock().map_err(|e| format!("Lock error: {}", e))?;
        let cutoff_time = Instant::now() - older_than;
        let mut removed_count = 0;

        let task_ids_to_remove: Vec<u64> = tasks
            .iter()
            .filter(|(_, task)| {
                matches!(task.status, TaskStatus::Completed | TaskStatus::Failed(_) | TaskStatus::Cancelled)
                    && task.completed_at.map_or(false, |completed_at| completed_at < cutoff_time)
            })
            .map(|(id, _)| *id)
            .collect();

        for task_id in task_ids_to_remove {
            tasks.remove(&task_id);
            removed_count += 1;
        }

        Ok(removed_count)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_scheduler_creation() {
        let scheduler = Scheduler::new();
        let stats = scheduler.get_stats().unwrap();
        
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.pending_tasks, 0);
        assert_eq!(stats.running_tasks, 0);
        assert_eq!(stats.completed_tasks, 0);
    }

    #[test]
    fn test_task_scheduling() {
        let scheduler = Scheduler::new();
        
        let task_id = scheduler.schedule_task("Test Task".to_string(), Priority::Normal).unwrap();
        assert_eq!(task_id, 1);
        
        let stats = scheduler.get_stats().unwrap();
        assert_eq!(stats.total_tasks, 1);
        assert_eq!(stats.pending_tasks, 1);
    }

    #[test]
    fn test_task_execution_flow() {
        let scheduler = Scheduler::new();
        
        let task_id = scheduler.schedule_task("Test Task".to_string(), Priority::High).unwrap();
        
        // Start the task
        scheduler.start_task(task_id).unwrap();
        let stats = scheduler.get_stats().unwrap();
        assert_eq!(stats.running_tasks, 1);
        assert_eq!(stats.pending_tasks, 0);
        
        // Complete the task
        thread::sleep(Duration::from_millis(10)); // Small delay to measure execution time
        scheduler.complete_task(task_id).unwrap();
        
        let stats = scheduler.get_stats().unwrap();
        assert_eq!(stats.completed_tasks, 1);
        assert_eq!(stats.running_tasks, 0);
        assert!(stats.average_execution_time > Duration::new(0, 0));
    }

    #[test]
    fn test_task_failure() {
        let scheduler = Scheduler::new();
        
        let task_id = scheduler.schedule_task("Failing Task".to_string(), Priority::Low).unwrap();
        scheduler.start_task(task_id).unwrap();
        scheduler.fail_task(task_id, "Test error".to_string()).unwrap();
        
        let stats = scheduler.get_stats().unwrap();
        assert_eq!(stats.failed_tasks, 1);
        
        let task = scheduler.get_task(task_id).unwrap().unwrap();
        if let TaskStatus::Failed(error) = task.status {
            assert_eq!(error, "Test error");
        } else {
            panic!("Task should be in failed state");
        }
    }

    #[test]
    fn test_task_cancellation() {
        let scheduler = Scheduler::new();
        
        let task_id = scheduler.schedule_task("Cancelled Task".to_string(), Priority::Critical).unwrap();
        scheduler.cancel_task(task_id).unwrap();
        
        let stats = scheduler.get_stats().unwrap();
        assert_eq!(stats.cancelled_tasks, 1);
    }

    #[test]
    fn test_priority_statistics() {
        let scheduler = Scheduler::new();
        
        scheduler.schedule_task("Low Priority".to_string(), Priority::Low).unwrap();
        scheduler.schedule_task("Normal Priority".to_string(), Priority::Normal).unwrap();
        scheduler.schedule_task("High Priority".to_string(), Priority::High).unwrap();
        scheduler.schedule_task("Critical Priority".to_string(), Priority::Critical).unwrap();
        
        let stats = scheduler.get_stats().unwrap();
        assert_eq!(stats.tasks_by_priority.get(&Priority::Low), Some(&1));
        assert_eq!(stats.tasks_by_priority.get(&Priority::Normal), Some(&1));
        assert_eq!(stats.tasks_by_priority.get(&Priority::High), Some(&1));
        assert_eq!(stats.tasks_by_priority.get(&Priority::Critical), Some(&1));
    }

    #[test]
    fn test_comprehensive_statistics() {
        let scheduler = Scheduler::new();
        
        // Create various tasks
        let task1 = scheduler.schedule_task("Task 1".to_string(), Priority::High).unwrap();
        let task2 = scheduler.schedule_task("Task 2".to_string(), Priority::Normal).unwrap();
        let task3 = scheduler.schedule_task("Task 3".to_string(), Priority::Low).unwrap();
        
        // Execute first task
        scheduler.start_task(task1).unwrap();
        thread::sleep(Duration::from_millis(5));
        scheduler.complete_task(task1).unwrap();
        
        // Fail second task
        scheduler.start_task(task2).unwrap();
        scheduler.fail_task(task2, "Simulated failure".to_string()).unwrap();
        
        // Leave third task pending
        let _task3 = task3; // Acknowledge the variable is intentionally unused in this test
        
        let stats = scheduler.get_stats().unwrap();
        
        assert_eq!(stats.total_tasks, 3);
        assert_eq!(stats.pending_tasks, 1);
        assert_eq!(stats.running_tasks, 0);
        assert_eq!(stats.completed_tasks, 1);
        assert_eq!(stats.failed_tasks, 1);
        assert_eq!(stats.cancelled_tasks, 0);
        
        assert!(stats.uptime > Duration::new(0, 0));
        assert!(stats.last_task_completed.is_some());
        assert!(stats.average_execution_time > Duration::new(0, 0));
    }

    #[test]
    fn test_cleanup_old_tasks() {
        let scheduler = Scheduler::new();
        
        let task_id = scheduler.schedule_task("Old Task".to_string(), Priority::Normal).unwrap();
        scheduler.start_task(task_id).unwrap();
        scheduler.complete_task(task_id).unwrap();
        
        // Try to cleanup tasks older than 1 second (should not remove anything yet)
        let removed = scheduler.cleanup_old_tasks(Duration::from_secs(1)).unwrap();
        assert_eq!(removed, 0);
        
        // Try to cleanup tasks older than 0 milliseconds (should remove the completed task)
        let removed = scheduler.cleanup_old_tasks(Duration::from_millis(0)).unwrap();
        assert_eq!(removed, 1);
        
        let stats = scheduler.get_stats().unwrap();
        assert_eq!(stats.total_tasks, 0);
    }
}