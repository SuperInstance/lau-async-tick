//! # lau-async-tick
//!
//! THE async tick engine — non-blocking tick processing with priority queues.

use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// TickPriority
// ---------------------------------------------------------------------------

/// Priority ordering for tick tasks. Critical(N) is highest; Background lowest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum TickPriority {
    Critical(u32),
    High,
    #[default]
    Normal,
    Low,
    Background,
}

impl TickPriority {
    fn order_value(&self) -> u32 {
        match self {
            TickPriority::Critical(n) => 2000 + *n,
            TickPriority::High => 1000,
            TickPriority::Normal => 500,
            TickPriority::Low => 100,
            TickPriority::Background => 0,
        }
    }
}

impl Ord for TickPriority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order_value().cmp(&other.order_value())
    }
}

impl PartialOrd for TickPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ---------------------------------------------------------------------------
// TickTaskType
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TickTaskType {
    MessageReceived,
    MessageRouted,
    RoomGravityUpdate,
    EnsignTick,
    DeadbandCheck,
    CorrelationScan,
    ProvenanceRecord,
    ConservationCheck,
    PhoneAFriend,
    StandDown,
    Escalation,
    BootstrapRoom,
}

impl TickTaskType {
    pub fn is_urgent(&self) -> bool {
        matches!(self, TickTaskType::Escalation | TickTaskType::MessageReceived)
    }
}

// ---------------------------------------------------------------------------
// TickTask
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickTask {
    pub id: String,
    pub task_type: TickTaskType,
    pub priority: TickPriority,
    pub created_at: u64,
    pub data: HashMap<String, String>,
}

static TASK_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_task_id() -> String {
    let n = TASK_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("task-{n}")
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl TickTask {
    pub fn new(task_type: TickTaskType) -> Self {
        Self {
            id: next_task_id(),
            task_type,
            priority: TickPriority::Normal,
            created_at: now_ms(),
            data: HashMap::new(),
        }
    }

    pub fn with_data(mut self, k: &str, v: &str) -> Self {
        self.data.insert(k.to_string(), v.to_string());
        self
    }

    pub fn with_priority(mut self, p: TickPriority) -> Self {
        self.priority = p;
        self
    }
}

impl PartialEq for TickTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TickTask {}

impl Ord for TickTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for TickTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ---------------------------------------------------------------------------
// TickError
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TickError {
    HandlerFailed(String),
    ContextMissing(String),
    BudgetExceeded(String),
    UnknownTaskType(String),
}

impl std::fmt::Display for TickError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TickError::HandlerFailed(s) => write!(f, "handler failed: {s}"),
            TickError::ContextMissing(s) => write!(f, "context missing: {s}"),
            TickError::BudgetExceeded(s) => write!(f, "budget exceeded: {s}"),
            TickError::UnknownTaskType(s) => write!(f, "unknown task type: {s}"),
        }
    }
}

impl std::error::Error for TickError {}

// ---------------------------------------------------------------------------
// TickAction
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TickAction {
    Processed(String),
    Delegated { to: String, task: Box<TickTask> },
    Escalated { reason: String, room: Option<String> },
    Deferred(u64),
    Noop,
}

impl TickAction {
    pub fn describe(&self) -> String {
        match self {
            TickAction::Processed(msg) => format!("Processed: {msg}"),
            TickAction::Delegated { to, .. } => format!("Delegated to {to}"),
            TickAction::Escalated { reason, room } => {
                format!("Escalated: {reason}{}", room.as_deref().map(|r| format!(" (room: {r})")).unwrap_or_default())
            }
            TickAction::Deferred(ticks) => format!("Deferred for {ticks} ticks"),
            TickAction::Noop => "Noop".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// TickContext — shared state for handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoomState {
    pub id: String,
    pub gravity: f64,
    pub alert: String,
    pub tile_count: u32,
    pub last_active: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnsignState {
    pub id: String,
    pub status: String,
    pub room: Option<String>,
    pub energy_remaining: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TickContext {
    pub tick: u64,
    pub rooms: HashMap<String, RoomState>,
    pub ensigns: HashMap<String, EnsignState>,
    pub conservation_remaining: f64,
    pub active_correlations: usize,
}

// ---------------------------------------------------------------------------
// TickHandler trait
// ---------------------------------------------------------------------------

pub trait TickHandler: Send + Sync {
    fn task_type(&self) -> TickTaskType;
    fn handle(&self, task: &TickTask, ctx: &TickContext) -> Result<TickAction, TickError>;
}

// ---------------------------------------------------------------------------
// TickResult
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickResult {
    pub tick: u64,
    pub tasks_processed: u32,
    pub tasks_deferred: u32,
    pub tasks_escalated: u32,
    pub actions: Vec<(TickTaskType, TickAction)>,
    pub duration_us: u64,
}

impl TickResult {
    pub fn summary(&self) -> String {
        format!(
            "tick {} | processed: {} | deferred: {} | escalated: {} | actions: {} | {}µs",
            self.tick,
            self.tasks_processed,
            self.tasks_deferred,
            self.tasks_escalated,
            self.actions.len(),
            self.duration_us,
        )
    }
}

// ---------------------------------------------------------------------------
// TickEngineStats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TickEngineStats {
    pub total_ticks: u64,
    pub total_tasks_processed: u64,
    pub total_escalations: u64,
    pub avg_tasks_per_tick: f64,
    pub avg_duration_us: u64,
    pub by_type: HashMap<TickTaskType, u64>,
}

// ---------------------------------------------------------------------------
// TickEngine — THE main loop
// ---------------------------------------------------------------------------

pub struct TickEngine {
    pub tick_interval_ms: u64,
    pending: Vec<TickTask>,
    priority_queue: BinaryHeap<TickTask>,
    handlers: HashMap<TickTaskType, Box<dyn TickHandler>>,
    tick_count: u64,
    pub start_time: u64,
    // stats accumulators
    total_tasks_processed: u64,
    total_escalations: u64,
    total_duration_us: u64,
    by_type: HashMap<TickTaskType, u64>,
    // context
    context: TickContext,
}

impl TickEngine {
    pub fn new(interval_ms: u64) -> Self {
        Self {
            tick_interval_ms: interval_ms,
            pending: Vec::new(),
            priority_queue: BinaryHeap::new(),
            handlers: HashMap::new(),
            tick_count: 0,
            start_time: now_ms(),
            total_tasks_processed: 0,
            total_escalations: 0,
            total_duration_us: 0,
            by_type: HashMap::new(),
            context: TickContext::default(),
        }
    }

    pub fn register_handler(&mut self, task_type: TickTaskType, handler: Box<dyn TickHandler>) {
        self.handlers.insert(task_type, handler);
    }

    pub fn enqueue(&mut self, task: TickTask) {
        self.priority_queue.push(task);
    }

    pub fn context_mut(&mut self) -> &mut TickContext {
        &mut self.context
    }

    pub fn set_context(&mut self, ctx: TickContext) {
        self.context = ctx;
    }

    /// Process one tick cycle: drain priority queue through handlers.
    pub fn tick(&mut self) -> TickResult {
        let t0 = std::time::Instant::now();
        self.tick_count += 1;
        self.context.tick = self.tick_count;

        // Move pending into priority queue
        for task in self.pending.drain(..) {
            self.priority_queue.push(task);
        }

        let mut result = TickResult {
            tick: self.tick_count,
            tasks_processed: 0,
            tasks_deferred: 0,
            tasks_escalated: 0,
            actions: Vec::new(),
            duration_us: 0,
        };

        // Collect delegated tasks to process after current batch
        let mut delegated_tasks: Vec<TickTask> = Vec::new();
        let mut batch_remaining = true;

        while batch_remaining {
            batch_remaining = false;
            // Process current queue
            while let Some(task) = self.priority_queue.pop() {
                let task_type = task.task_type.clone();
                let action = match self.handlers.get(&task_type) {
                    Some(handler) => handler.handle(&task, &self.context),
                    None => Err(TickError::UnknownTaskType(format!("{task_type:?}"))),
                };

                match action {
                    Ok(act) => {
                        match &act {
                            TickAction::Deferred(_) => result.tasks_deferred += 1,
                            TickAction::Escalated { .. } => {
                                result.tasks_escalated += 1;
                                self.total_escalations += 1;
                            }
                            TickAction::Delegated { task: delegated_task, .. } => {
                                delegated_tasks.push((**delegated_task).clone());
                            }
                            TickAction::Processed(_) | TickAction::Noop => {
                                result.tasks_processed += 1;
                                self.total_tasks_processed += 1;
                            }
                        }
                        *self.by_type.entry(task_type.clone()).or_insert(0) += 1;
                        result.actions.push((task_type, act));
                    }
                    Err(_) => {
                        result.tasks_processed += 1;
                        self.total_tasks_processed += 1;
                        *self.by_type.entry(task_type.clone()).or_insert(0) += 1;
                        result.actions.push((task_type, TickAction::Processed("error".to_string())));
                    }
                }
            }

            // Re-enqueue delegated tasks and loop
            if !delegated_tasks.is_empty() {
                for task in delegated_tasks.drain(..) {
                    self.priority_queue.push(task);
                }
                batch_remaining = true;
            }
        }

        let elapsed = t0.elapsed().as_micros() as u64;
        result.duration_us = elapsed;
        self.total_duration_us += elapsed;

        result
    }

    /// Run multiple ticks, optionally capped.
    pub fn run(&mut self, max_ticks: Option<u64>) -> Vec<TickResult> {
        let limit = max_ticks.unwrap_or(u64::MAX);
        let mut results = Vec::new();
        for _ in 0..limit {
            if self.priority_queue.is_empty() && self.pending.is_empty() {
                break;
            }
            results.push(self.tick());
        }
        results
    }

    /// Process all pending tasks in one or more ticks until drained.
    pub fn drain(&mut self) -> Vec<TickResult> {
        self.run(None)
    }

    pub fn stats(&self) -> TickEngineStats {
        let avg_tasks = if self.tick_count > 0 {
            self.total_tasks_processed as f64 / self.tick_count as f64
        } else {
            0.0
        };
        let avg_dur = self.total_duration_us.checked_div(self.tick_count).unwrap_or(0);
        TickEngineStats {
            total_ticks: self.tick_count,
            total_tasks_processed: self.total_tasks_processed,
            total_escalations: self.total_escalations,
            avg_tasks_per_tick: avg_tasks,
            avg_duration_us: avg_dur,
            by_type: self.by_type.clone(),
        }
    }

    pub fn queue_len(&self) -> usize {
        self.priority_queue.len() + self.pending.len()
    }
}

// ---------------------------------------------------------------------------
// Pre-built handlers
// ---------------------------------------------------------------------------

/// Checks deadbands — escalates if gravity exceeds threshold.
pub struct DeadbandCheckHandler {
    pub threshold: f64,
}

impl DeadbandCheckHandler {
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }
}

impl TickHandler for DeadbandCheckHandler {
    fn task_type(&self) -> TickTaskType {
        TickTaskType::DeadbandCheck
    }

    fn handle(&self, task: &TickTask, ctx: &TickContext) -> Result<TickAction, TickError> {
        let room_id = task.data.get("room_id").cloned().unwrap_or_default();
        if let Some(room) = ctx.rooms.get(&room_id) {
            if room.gravity.abs() > self.threshold {
                return Ok(TickAction::Escalated {
                    reason: format!("deadband breach: gravity={:.4} > threshold={:.4}", room.gravity, self.threshold),
                    room: Some(room_id),
                });
            }
            Ok(TickAction::Processed(format!("deadband ok for room {room_id}")))
        } else {
            Err(TickError::ContextMissing(format!("room {room_id}")))
        }
    }
}

/// Decays room gravities toward neutral (0.0) by a decay factor.
pub struct GravityDecayHandler {
    pub decay_factor: f64,
}

impl GravityDecayHandler {
    pub fn new(decay_factor: f64) -> Self {
        Self { decay_factor }
    }
}

impl TickHandler for GravityDecayHandler {
    fn task_type(&self) -> TickTaskType {
        TickTaskType::RoomGravityUpdate
    }

    fn handle(&self, task: &TickTask, _ctx: &TickContext) -> Result<TickAction, TickError> {
        let room_id = task.data.get("room_id").cloned().unwrap_or_default();
        if let Some(gravity_str) = task.data.get("gravity") {
            let gravity: f64 = gravity_str.parse().unwrap_or(0.0);
            let decayed = gravity * self.decay_factor;
            Ok(TickAction::Processed(format!(
                "room {room_id} gravity decayed: {gravity:.4} -> {decayed:.4}"
            )))
        } else {
            Ok(TickAction::Processed(format!("room {room_id} no gravity to decay")))
        }
    }
}

/// Checks conservation budget — escalates if remaining drops below threshold.
pub struct ConservationCheckHandler {
    pub warn_threshold: f64,
}

impl ConservationCheckHandler {
    pub fn new(warn_threshold: f64) -> Self {
        Self { warn_threshold }
    }
}

impl TickHandler for ConservationCheckHandler {
    fn task_type(&self) -> TickTaskType {
        TickTaskType::ConservationCheck
    }

    fn handle(&self, _task: &TickTask, ctx: &TickContext) -> Result<TickAction, TickError> {
        if ctx.conservation_remaining < self.warn_threshold {
            Ok(TickAction::Escalated {
                reason: format!(
                    "conservation budget low: {:.2} < {:.2}",
                    ctx.conservation_remaining, self.warn_threshold
                ),
                room: None,
            })
        } else {
            Ok(TickAction::Processed(format!(
                "conservation ok: {:.2}",
                ctx.conservation_remaining
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- TickPriority tests --

    #[test]
    fn priority_ordering() {
        assert!(TickPriority::Critical(1) > TickPriority::High);
        assert!(TickPriority::High > TickPriority::Normal);
        assert!(TickPriority::Normal > TickPriority::Low);
        assert!(TickPriority::Low > TickPriority::Background);
        assert!(TickPriority::Critical(10) > TickPriority::Critical(1));
    }

    #[test]
    fn priority_order_value() {
        assert_eq!(TickPriority::Critical(5).order_value(), 2005);
        assert_eq!(TickPriority::High.order_value(), 1000);
        assert_eq!(TickPriority::Normal.order_value(), 500);
        assert_eq!(TickPriority::Low.order_value(), 100);
        assert_eq!(TickPriority::Background.order_value(), 0);
    }

    #[test]
    fn priority_default() {
        assert_eq!(TickPriority::default(), TickPriority::Normal);
    }

    #[test]
    fn priority_serde_roundtrip() {
        let p = TickPriority::Critical(42);
        let json = serde_json::to_string(&p).unwrap();
        let back: TickPriority = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }

    // -- TickTaskType tests --

    #[test]
    fn urgent_types() {
        assert!(TickTaskType::Escalation.is_urgent());
        assert!(TickTaskType::MessageReceived.is_urgent());
        assert!(!TickTaskType::DeadbandCheck.is_urgent());
        assert!(!TickTaskType::BootstrapRoom.is_urgent());
        assert!(!TickTaskType::CorrelationScan.is_urgent());
    }

    #[test]
    fn task_type_serde_roundtrip() {
        let tt = TickTaskType::PhoneAFriend;
        let json = serde_json::to_string(&tt).unwrap();
        let back: TickTaskType = serde_json::from_str(&json).unwrap();
        assert_eq!(tt, back);
    }

    // -- TickTask tests --

    #[test]
    fn task_new() {
        let task = TickTask::new(TickTaskType::EnsignTick);
        assert!(task.id.starts_with("task-"));
        assert_eq!(task.task_type, TickTaskType::EnsignTick);
        assert_eq!(task.priority, TickPriority::Normal);
        assert!(task.data.is_empty());
    }

    #[test]
    fn task_builder_pattern() {
        let task = TickTask::new(TickTaskType::MessageReceived)
            .with_priority(TickPriority::Critical(1))
            .with_data("room_id", "room-42")
            .with_data("msg", "hello");
        assert_eq!(task.priority, TickPriority::Critical(1));
        assert_eq!(task.data.get("room_id").unwrap(), "room-42");
        assert_eq!(task.data.get("msg").unwrap(), "hello");
    }

    #[test]
    fn task_equality_by_id() {
        let t1 = TickTask::new(TickTaskType::MessageReceived);
        let t2 = t1.clone();
        assert_eq!(t1, t2);
    }

    #[test]
    fn task_serde_roundtrip() {
        let task = TickTask::new(TickTaskType::BootstrapRoom)
            .with_data("key", "value")
            .with_priority(TickPriority::High);
        let json = serde_json::to_string(&task).unwrap();
        let back: TickTask = serde_json::from_str(&json).unwrap();
        assert_eq!(task.id, back.id);
        assert_eq!(task.task_type, back.task_type);
        assert_eq!(task.priority, back.priority);
    }

    // -- TickAction tests --

    #[test]
    fn action_describe_processed() {
        let a = TickAction::Processed("done".to_string());
        assert_eq!(a.describe(), "Processed: done");
    }

    #[test]
    fn action_describe_delegated() {
        let a = TickAction::Delegated {
            to: "handler-x".to_string(),
            task: Box::new(TickTask::new(TickTaskType::StandDown)),
        };
        assert_eq!(a.describe(), "Delegated to handler-x");
    }

    #[test]
    fn action_describe_escalated() {
        let a = TickAction::Escalated {
            reason: "breach".to_string(),
            room: Some("room-1".to_string()),
        };
        assert_eq!(a.describe(), "Escalated: breach (room: room-1)");
    }

    #[test]
    fn action_describe_escalated_no_room() {
        let a = TickAction::Escalated {
            reason: "budget".to_string(),
            room: None,
        };
        assert_eq!(a.describe(), "Escalated: budget");
    }

    #[test]
    fn action_describe_deferred() {
        let a = TickAction::Deferred(5);
        assert_eq!(a.describe(), "Deferred for 5 ticks");
    }

    #[test]
    fn action_describe_noop() {
        assert_eq!(TickAction::Noop.describe(), "Noop");
    }

    // -- TickError tests --

    #[test]
    fn error_display() {
        assert_eq!(
            TickError::HandlerFailed("oops".to_string()).to_string(),
            "handler failed: oops"
        );
        assert_eq!(
            TickError::ContextMissing("room".to_string()).to_string(),
            "context missing: room"
        );
        assert_eq!(
            TickError::BudgetExceeded("100".to_string()).to_string(),
            "budget exceeded: 100"
        );
        assert_eq!(
            TickError::UnknownTaskType("Foo".to_string()).to_string(),
            "unknown task type: Foo"
        );
    }

    #[test]
    fn error_equality() {
        assert_eq!(
            TickError::HandlerFailed("a".to_string()),
            TickError::HandlerFailed("a".to_string())
        );
        assert_ne!(
            TickError::HandlerFailed("a".to_string()),
            TickError::HandlerFailed("b".to_string())
        );
    }

    // -- TickResult tests --

    #[test]
    fn result_summary() {
        let r = TickResult {
            tick: 3,
            tasks_processed: 5,
            tasks_deferred: 1,
            tasks_escalated: 0,
            actions: vec![(TickTaskType::EnsignTick, TickAction::Noop)],
            duration_us: 42,
        };
        let s = r.summary();
        assert!(s.contains("tick 3"));
        assert!(s.contains("processed: 5"));
        assert!(s.contains("deferred: 1"));
        assert!(s.contains("42µs"));
    }

    // -- TickContext / RoomState / EnsignState tests --

    #[test]
    fn context_default() {
        let ctx = TickContext::default();
        assert_eq!(ctx.tick, 0);
        assert!(ctx.rooms.is_empty());
        assert!(ctx.ensigns.is_empty());
        assert_eq!(ctx.conservation_remaining, 0.0);
        assert_eq!(ctx.active_correlations, 0);
    }

    #[test]
    fn room_state_default() {
        let r = RoomState::default();
        assert_eq!(r.gravity, 0.0);
        assert_eq!(r.tile_count, 0);
    }

    #[test]
    fn ensign_state_default() {
        let e = EnsignState::default();
        assert!(e.room.is_none());
        assert_eq!(e.energy_remaining, 0.0);
    }

    #[test]
    fn context_serde_roundtrip() {
        let ctx = TickContext {
            tick: 7,
            rooms: {
                let mut m = HashMap::new();
                m.insert("r1".to_string(), RoomState {
                    id: "r1".to_string(),
                    gravity: 0.5,
                    alert: "hot".to_string(),
                    tile_count: 3,
                    last_active: 100,
                });
                m
            },
            ensigns: HashMap::new(),
            conservation_remaining: 99.0,
            active_correlations: 2,
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let back: TickContext = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx.tick, back.tick);
        assert_eq!(ctx.conservation_remaining, back.conservation_remaining);
    }

    // -- TickEngine core tests --

    #[test]
    fn engine_new() {
        let engine = TickEngine::new(1000);
        assert_eq!(engine.tick_interval_ms, 1000);
        assert_eq!(engine.queue_len(), 0);
    }

    #[test]
    fn engine_enqueue_and_queue_len() {
        let mut engine = TickEngine::new(500);
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.enqueue(TickTask::new(TickTaskType::DeadbandCheck));
        assert_eq!(engine.queue_len(), 2);
    }

    #[test]
    fn engine_tick_empty_queue() {
        let mut engine = TickEngine::new(100);
        let result = engine.tick();
        assert_eq!(result.tick, 1);
        assert_eq!(result.tasks_processed, 0);
        assert!(result.actions.is_empty());
    }

    #[test]
    fn engine_tick_with_unknown_handler() {
        let mut engine = TickEngine::new(100);
        engine.enqueue(TickTask::new(TickTaskType::CorrelationScan));
        let result = engine.tick();
        assert_eq!(result.tasks_processed, 1);
    }

    #[test]
    fn engine_tick_processes_registered_handler() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::EnsignTick,
            Box::new(NoopHandler(TickTaskType::EnsignTick)),
        );
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        let result = engine.tick();
        assert_eq!(result.tasks_processed, 1);
        assert_eq!(result.actions.len(), 1);
    }

    #[test]
    fn engine_priority_ordering() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::MessageReceived,
            Box::new(NoopHandler(TickTaskType::MessageReceived)),
        );
        engine.register_handler(
            TickTaskType::CorrelationScan,
            Box::new(NoopHandler(TickTaskType::CorrelationScan)),
        );

        // Low priority first, then critical
        let low = TickTask::new(TickTaskType::CorrelationScan).with_priority(TickPriority::Low);
        let crit = TickTask::new(TickTaskType::MessageReceived).with_priority(TickPriority::Critical(1));

        engine.enqueue(low);
        engine.enqueue(crit);

        let result = engine.tick();
        // Critical should be processed first
        assert_eq!(result.actions[0].0, TickTaskType::MessageReceived);
        assert_eq!(result.actions[1].0, TickTaskType::CorrelationScan);
    }

    #[test]
    fn engine_run_with_max_ticks() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::EnsignTick,
            Box::new(NoopHandler(TickTaskType::EnsignTick)),
        );
        // Put 1 task, tick, then add 2 more — gives 2 ticks
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.tick();
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        let results = engine.run(Some(10));
        assert_eq!(results.len(), 1); // 2nd tick drains both
        assert_eq!(engine.tick_count, 2);
    }

    #[test]
    fn engine_drain() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::EnsignTick,
            Box::new(NoopHandler(TickTaskType::EnsignTick)),
        );
        for _ in 0..3 {
            engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        }
        let results = engine.drain();
        assert_eq!(results.len(), 1); // all 3 in one tick
        assert_eq!(results[0].tasks_processed, 3);
    }

    #[test]
    fn engine_stats() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::EnsignTick,
            Box::new(NoopHandler(TickTaskType::EnsignTick)),
        );
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.tick();

        let stats = engine.stats();
        assert_eq!(stats.total_ticks, 1);
        assert_eq!(stats.total_tasks_processed, 2);
        assert_eq!(stats.avg_tasks_per_tick, 2.0);
        assert_eq!(stats.by_type.get(&TickTaskType::EnsignTick), Some(&2));
    }

    #[test]
    fn engine_stats_empty() {
        let engine = TickEngine::new(100);
        let stats = engine.stats();
        assert_eq!(stats.total_ticks, 0);
        assert_eq!(stats.total_tasks_processed, 0);
        assert_eq!(stats.avg_tasks_per_tick, 0.0);
        assert_eq!(stats.avg_duration_us, 0);
    }

    #[test]
    fn engine_tick_count_increments() {
        let mut engine = TickEngine::new(100);
        assert_eq!(engine.tick_count, 0);
        engine.tick();
        assert_eq!(engine.tick_count, 1);
        engine.tick();
        assert_eq!(engine.tick_count, 2);
    }

    #[test]
    fn engine_context_mut() {
        let mut engine = TickEngine::new(100);
        engine.context_mut().conservation_remaining = 50.0;
        assert_eq!(engine.context.conservation_remaining, 50.0);
    }

    #[test]
    fn engine_set_context() {
        let mut engine = TickEngine::new(100);
        let mut ctx = TickContext::default();
        ctx.tick = 99;
        engine.set_context(ctx);
        // tick() will overwrite context.tick
        engine.tick();
        assert_eq!(engine.context.tick, 1);
    }

    #[test]
    fn engine_delegated_task_reenqueued() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::MessageReceived,
            Box::new(DelegateHandler {
                target_type: TickTaskType::MessageRouted,
            }),
        );
        engine.register_handler(
            TickTaskType::MessageRouted,
            Box::new(NoopHandler(TickTaskType::MessageRouted)),
        );
        engine.enqueue(TickTask::new(TickTaskType::MessageReceived));
        let result = engine.tick();
        // Delegated (MessageReceived) + Processed (MessageRouted) = 2 actions, 1 processed
        assert_eq!(result.actions.len(), 2);
        assert_eq!(result.tasks_processed, 1);
        assert_eq!(result.actions[0].0, TickTaskType::MessageReceived);
        assert_eq!(result.actions[1].0, TickTaskType::MessageRouted);
    }

    #[test]
    fn engine_escalation_counted() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::Escalation,
            Box::new(EscalateHandler),
        );
        engine.enqueue(TickTask::new(TickTaskType::Escalation));
        let result = engine.tick();
        assert_eq!(result.tasks_escalated, 1);
        let stats = engine.stats();
        assert_eq!(stats.total_escalations, 1);
    }

    // -- DeadbandCheckHandler tests --

    #[test]
    fn deadband_ok() {
        let handler = DeadbandCheckHandler::new(1.0);
        let task = TickTask::new(TickTaskType::DeadbandCheck).with_data("room_id", "r1");
        let ctx = TickContext {
            rooms: {
                let mut m = HashMap::new();
                m.insert("r1".to_string(), RoomState { id: "r1".to_string(), gravity: 0.5, ..Default::default() });
                m
            },
            ..Default::default()
        };
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Processed(msg) => assert!(msg.contains("deadband ok")),
            _ => panic!("expected Processed"),
        }
    }

    #[test]
    fn deadband_breach() {
        let handler = DeadbandCheckHandler::new(1.0);
        let task = TickTask::new(TickTaskType::DeadbandCheck).with_data("room_id", "r1");
        let ctx = TickContext {
            rooms: {
                let mut m = HashMap::new();
                m.insert("r1".to_string(), RoomState { id: "r1".to_string(), gravity: 2.0, ..Default::default() });
                m
            },
            ..Default::default()
        };
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Escalated { reason, room } => {
                assert!(reason.contains("deadband breach"));
                assert_eq!(room, Some("r1".to_string()));
            }
            _ => panic!("expected Escalated"),
        }
    }

    #[test]
    fn deadband_missing_room() {
        let handler = DeadbandCheckHandler::new(1.0);
        let task = TickTask::new(TickTaskType::DeadbandCheck).with_data("room_id", "missing");
        let ctx = TickContext::default();
        let result = handler.handle(&task, &ctx);
        assert!(result.is_err());
        match result.unwrap_err() {
            TickError::ContextMissing(s) => assert!(s.contains("missing")),
            _ => panic!("expected ContextMissing"),
        }
    }

    #[test]
    fn deadband_negative_gravity() {
        let handler = DeadbandCheckHandler::new(1.0);
        let task = TickTask::new(TickTaskType::DeadbandCheck).with_data("room_id", "r1");
        let ctx = TickContext {
            rooms: {
                let mut m = HashMap::new();
                m.insert("r1".to_string(), RoomState { id: "r1".to_string(), gravity: -1.5, ..Default::default() });
                m
            },
            ..Default::default()
        };
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Escalated { .. } => {}
            _ => panic!("expected Escalated for negative gravity"),
        }
    }

    // -- GravityDecayHandler tests --

    #[test]
    fn gravity_decay() {
        let handler = GravityDecayHandler::new(0.9);
        let task = TickTask::new(TickTaskType::RoomGravityUpdate)
            .with_data("room_id", "r1")
            .with_data("gravity", "1.0");
        let ctx = TickContext::default();
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Processed(msg) => {
                assert!(msg.contains("0.9000"));
            }
            _ => panic!("expected Processed"),
        }
    }

    #[test]
    fn gravity_decay_no_gravity_data() {
        let handler = GravityDecayHandler::new(0.9);
        let task = TickTask::new(TickTaskType::RoomGravityUpdate).with_data("room_id", "r1");
        let ctx = TickContext::default();
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Processed(msg) => assert!(msg.contains("no gravity")),
            _ => panic!("expected Processed"),
        }
    }

    // -- ConservationCheckHandler tests --

    #[test]
    fn conservation_ok() {
        let handler = ConservationCheckHandler::new(10.0);
        let task = TickTask::new(TickTaskType::ConservationCheck);
        let ctx = TickContext {
            conservation_remaining: 50.0,
            ..Default::default()
        };
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Processed(msg) => assert!(msg.contains("conservation ok")),
            _ => panic!("expected Processed"),
        }
    }

    #[test]
    fn conservation_low() {
        let handler = ConservationCheckHandler::new(10.0);
        let task = TickTask::new(TickTaskType::ConservationCheck);
        let ctx = TickContext {
            conservation_remaining: 5.0,
            ..Default::default()
        };
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Escalated { reason, room } => {
                assert!(reason.contains("conservation budget low"));
                assert!(room.is_none());
            }
            _ => panic!("expected Escalated"),
        }
    }

    #[test]
    fn conservation_exactly_at_threshold() {
        let handler = ConservationCheckHandler::new(10.0);
        let task = TickTask::new(TickTaskType::ConservationCheck);
        let ctx = TickContext {
            conservation_remaining: 10.0,
            ..Default::default()
        };
        let result = handler.handle(&task, &ctx).unwrap();
        match result {
            TickAction::Processed(_) => {} // not below threshold
            _ => panic!("expected Processed at exact threshold"),
        }
    }

    // -- Integration: full pipeline --

    #[test]
    fn full_pipeline() {
        let mut engine = TickEngine::new(100);

        engine.register_handler(
            TickTaskType::DeadbandCheck,
            Box::new(DeadbandCheckHandler::new(1.0)),
        );
        engine.register_handler(
            TickTaskType::RoomGravityUpdate,
            Box::new(GravityDecayHandler::new(0.95)),
        );
        engine.register_handler(
            TickTaskType::ConservationCheck,
            Box::new(ConservationCheckHandler::new(20.0)),
        );

        engine.context_mut().conservation_remaining = 50.0;
        engine.context_mut().rooms.insert(
            "room-1".to_string(),
            RoomState {
                id: "room-1".to_string(),
                gravity: 0.3,
                tile_count: 4,
                ..Default::default()
            },
        );

        engine.enqueue(
            TickTask::new(TickTaskType::DeadbandCheck)
                .with_data("room_id", "room-1")
                .with_priority(TickPriority::High),
        );
        engine.enqueue(
            TickTask::new(TickTaskType::RoomGravityUpdate)
                .with_data("room_id", "room-1")
                .with_data("gravity", "0.3"),
        );
        engine.enqueue(TickTask::new(TickTaskType::ConservationCheck));

        let result = engine.tick();
        assert_eq!(result.tasks_processed, 3);
        assert_eq!(result.actions.len(), 3);

        let stats = engine.stats();
        assert_eq!(stats.total_ticks, 1);
        assert_eq!(stats.total_tasks_processed, 3);
    }

    #[test]
    fn multi_tick_pipeline() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::EnsignTick,
            Box::new(NoopHandler(TickTaskType::EnsignTick)),
        );

        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.tick();

        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.tick();

        let stats = engine.stats();
        assert_eq!(stats.total_ticks, 2);
        assert_eq!(stats.total_tasks_processed, 3);
        assert!((stats.avg_tasks_per_tick - 1.5).abs() < 0.01);
    }

    #[test]
    fn engine_run_stops_when_empty() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::EnsignTick,
            Box::new(NoopHandler(TickTaskType::EnsignTick)),
        );
        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        // run with max 100 but only 1 task
        let results = engine.run(Some(100));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn stats_by_type_multiple() {
        let mut engine = TickEngine::new(100);
        engine.register_handler(
            TickTaskType::EnsignTick,
            Box::new(NoopHandler(TickTaskType::EnsignTick)),
        );
        engine.register_handler(
            TickTaskType::DeadbandCheck,
            Box::new(DeadbandCheckHandler::new(1.0)),
        );
        engine.context_mut().rooms.insert(
            "r1".to_string(),
            RoomState { id: "r1".to_string(), gravity: 0.1, ..Default::default() },
        );

        engine.enqueue(TickTask::new(TickTaskType::EnsignTick));
        engine.enqueue(TickTask::new(TickTaskType::DeadbandCheck).with_data("room_id", "r1"));
        engine.tick();

        let stats = engine.stats();
        assert_eq!(stats.by_type.len(), 2);
        assert_eq!(stats.by_type.get(&TickTaskType::EnsignTick), Some(&1));
        assert_eq!(stats.by_type.get(&TickTaskType::DeadbandCheck), Some(&1));
    }

    // -- Additional tests for coverage --

    #[test]
    fn task_created_at_populated() {
        let task = TickTask::new(TickTaskType::BootstrapRoom);
        assert!(task.created_at > 0);
    }

    #[test]
    fn tick_action_serde_roundtrip() {
        let action = TickAction::Escalated {
            reason: "test".to_string(),
            room: Some("r1".to_string()),
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: TickAction = serde_json::from_str(&json).unwrap();
        assert_eq!(action.describe(), back.describe());
    }

    #[test]
    fn tick_result_serde_roundtrip() {
        let r = TickResult {
            tick: 1,
            tasks_processed: 2,
            tasks_deferred: 0,
            tasks_escalated: 1,
            actions: vec![],
            duration_us: 100,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: TickResult = serde_json::from_str(&json).unwrap();
        assert_eq!(r.tick, back.tick);
        assert_eq!(r.duration_us, back.duration_us);
    }

    #[test]
    fn tick_error_serde_roundtrip() {
        let err = TickError::BudgetExceeded("100".to_string());
        let json = serde_json::to_string(&err).unwrap();
        let back: TickError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }

    // -- Test helper handlers --

    struct NoopHandler(TickTaskType);

    impl TickHandler for NoopHandler {
        fn task_type(&self) -> TickTaskType {
            self.0.clone()
        }
        fn handle(&self, _task: &TickTask, _ctx: &TickContext) -> Result<TickAction, TickError> {
            Ok(TickAction::Noop)
        }
    }

    struct DelegateHandler {
        target_type: TickTaskType,
    }

    impl TickHandler for DelegateHandler {
        fn task_type(&self) -> TickTaskType {
            TickTaskType::MessageReceived
        }
        fn handle(&self, task: &TickTask, _ctx: &TickContext) -> Result<TickAction, TickError> {
            Ok(TickAction::Delegated {
                to: format!("{:?}", self.target_type),
                task: Box::new(
                    TickTask::new(self.target_type.clone())
                        .with_data("from", &task.id),
                ),
            })
        }
    }

    struct EscalateHandler;

    impl TickHandler for EscalateHandler {
        fn task_type(&self) -> TickTaskType {
            TickTaskType::Escalation
        }
        fn handle(&self, _task: &TickTask, _ctx: &TickContext) -> Result<TickAction, TickError> {
            Ok(TickAction::Escalated {
                reason: "test escalation".to_string(),
                room: None,
            })
        }
    }
}
