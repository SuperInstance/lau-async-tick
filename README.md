# lau-async-tick

A synchronous tick-based task engine with priority queues, handler dispatch, delegation, and escalation — designed for game-agent simulation loops where tasks are processed in discrete ticks with configurable priority ordering.

> **55 tests** · depends on `serde` + `serde_json`

---

## What This Does

This crate provides a **tick engine** — a single-threaded, non-blocking task processor that:

1. **Enqueues** tasks with priorities into a binary heap.
2. **Dispatches** each task to a registered handler on each tick cycle.
3. **Handles delegation**: a handler can spawn a new task that gets re-enqueued within the same tick cycle.
4. **Tracks escalations, deferrals, and errors** with per-tick and cumulative statistics.
5. **Shares context** (rooms, ensigns, conservation budgets) across all handlers via `TickContext`.

The engine is deliberately synchronous (no `async`) — each call to `tick()` drains the current priority queue in one batch. This makes it deterministic, easy to test, and suitable for game simulation steps.

---

## Key Idea

A **tick** is one discrete time step. During a tick:

1. All pending tasks are moved into a max-heap (priority queue).
2. Tasks are popped from highest to lowest priority and dispatched to their handler.
3. If a handler **delegates** (produces a new task), it's re-enqueued and processed in the *same* tick cycle.
4. Results are collected into a `TickResult` with counts and actions.

This gives you deterministic, priority-ordered task processing without the complexity of async runtimes. The engine tracks cumulative statistics (total tasks processed, escalation count, average tasks per tick, duration) via `TickEngineStats`.

---

## Install

```toml
[dependencies]
lau-async-tick = { git = "https://github.com/SuperInstance/lau-async-tick" }
```

Dependencies: `serde`, `serde_json`.

---

## Quick Start

```rust
use lau_async_tick::*;

// 1. Create an engine with a 100ms tick interval
let mut engine = TickEngine::new(100);

// 2. Register handlers
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

// 3. Set up context
engine.context_mut().conservation_remaining = 50.0;
engine.context_mut().rooms.insert(
    "room-1".to_string(),
    RoomState { id: "room-1".to_string(), gravity: 0.3, tile_count: 4, ..Default::default() },
);

// 4. Enqueue tasks with priorities
engine.enqueue(
    TickTask::new(TickTaskType::DeadbandCheck)
        .with_data("room_id", "room-1")
        .with_priority(TickPriority::High),
);
engine.enqueue(TickTask::new(TickTaskType::ConservationCheck));

// 5. Process one tick
let result = engine.tick();
println!("{}", result.summary());
// tick 1 | processed: 2 | deferred: 0 | escalated: 0 | actions: 2 | 15µs

// 6. Check cumulative stats
let stats = engine.stats();
println!("total tasks: {}", stats.total_tasks_processed);
```

---

## API Reference

### Core Types

| Type | Description |
|---|---|
| `TickEngine` | The main engine — holds the priority queue, handlers, context, and stats |
| `TickTask` | A unit of work with id, type, priority, timestamp, and key-value data |
| `TickResult` | Outcome of one tick: processed/deferred/escalated counts, actions, duration |
| `TickEngineStats` | Cumulative statistics across all ticks |
| `TickContext` | Shared mutable state: rooms, ensigns, conservation budget |
| `TickAction` | What a handler returns: Processed, Delegated, Escalated, Deferred, Noop |
| `TickError` | Handler errors: HandlerFailed, ContextMissing, BudgetExceeded, UnknownTaskType |

### Priority

```rust
pub enum TickPriority {
    Critical(u32),  // Highest: Critical(1) < Critical(10), order value = 2000 + n
    High,           // order value = 1000
    Normal,         // default, order value = 500
    Low,            // order value = 100
    Background,     // order value = 0
}
```

Tasks with higher priority values are processed first within a tick.

### Task Types

```rust
pub enum TickTaskType {
    MessageReceived,      // urgent
    MessageRouted,
    RoomGravityUpdate,
    EnsignTick,
    DeadbandCheck,
    CorrelationScan,
    ProvenanceRecord,
    ConservationCheck,
    PhoneAFriend,
    StandDown,
    Escalation,           // urgent
    BootstrapRoom,
}
```

`is_urgent()` returns true for `MessageReceived` and `Escalation`.

### TickEngine Methods

| Method | Description |
|---|---|
| `new(interval_ms)` | Create engine with given tick interval |
| `register_handler(type, handler)` | Register a `TickHandler` for a task type |
| `enqueue(task)` | Add task to the priority queue |
| `tick()` | Process one tick cycle → `TickResult` |
| `run(max_ticks)` | Run multiple ticks (until empty or max reached) |
| `drain()` | Process all pending tasks |
| `stats()` | Get cumulative `TickEngineStats` |
| `queue_len()` | Number of pending tasks |
| `context_mut()` | Mutable reference to shared `TickContext` |
| `set_context(ctx)` | Replace the entire context |

### TickHandler Trait

```rust
pub trait TickHandler: Send + Sync {
    fn task_type(&self) -> TickTaskType;
    fn handle(&self, task: &TickTask, ctx: &TickContext) -> Result<TickAction, TickError>;
}
```

### TickAction Variants

| Variant | Effect |
|---|---|
| `Processed(msg)` | Task completed, counted as processed |
| `Delegated { to, task }` | Spawn a new task — re-enqueued in the same tick cycle |
| `Escalated { reason, room }` | Task escalated, counted separately |
| `Deferred(ticks)` | Task deferred, not re-enqueued |
| `Noop` | No-op, counted as processed |

### Built-in Handlers

| Handler | Type | Description |
|---|---|---|
| `DeadbandCheckHandler` | `DeadbandCheck` | Escalates if room gravity exceeds threshold |
| `GravityDecayHandler` | `RoomGravityUpdate` | Decays gravity by a factor |
| `ConservationCheckHandler` | `ConservationCheck` | Escalates if budget drops below threshold |

### State Types

| Type | Fields |
|---|---|
| `RoomState` | id, gravity, alert, tile_count, last_active |
| `EnsignState` | id, status, room, energy_remaining |
| `TickContext` | tick, rooms (HashMap), ensigns (HashMap), conservation_remaining, active_correlations |

---

## How It Works

### Tick Cycle

```
┌─────────────────────────────────────┐
│              tick()                  │
│                                     │
│  1. Move pending → priority queue   │
│  2. While queue not empty:          │
│     a. Pop highest-priority task    │
│     b. Look up handler by type      │
│     c. handler.handle(task, ctx)    │
│     d. Collect TickAction           │
│     e. If Delegated → re-enqueue    │
│  3. Process delegated tasks (loop)  │
│  4. Return TickResult               │
└─────────────────────────────────────┘
```

Delegated tasks are collected during each pass and re-enqueued for another pass within the same tick. The loop continues until no new delegations occur (or the queue is empty).

### Priority Ordering

`TickPriority` implements `Ord` by mapping each level to a numeric value:

| Priority | Value |
|---|---|
| Background | 0 |
| Low | 100 |
| Normal | 500 |
| High | 1000 |
| Critical(n) | 2000 + n |

The `BinaryHeap<TickTask>` is a max-heap, so `Critical(10)` tasks are processed before `High`, which is before `Normal`, etc.

### Task IDs

Auto-generated via an atomic counter: `task-1`, `task-2`, … Task equality is by ID, not by content.

### Context

`TickContext` is shared immutably with all handlers during a tick (handlers receive `&TickContext`). To mutate context, use `context_mut()` between ticks.

---

## The Math

This crate is more engineering than mathematics, but the priority queue has an important theoretical property:

### BinaryHeap Complexity

- **Enqueue**: O(log n) amortized per task.
- **Dequeue** (pop max): O(log n).
- **Full tick drain** of m tasks: O(m log m).

The delegation loop adds at most one extra pass per delegation level. With d levels of delegation, a tick costs O(d · m log m).

### Conservation Law

The `ConservationCheckHandler` enforces a budget invariant:

$$\text{conservation\_remaining} \geq \text{warn\_threshold}$$

If violated, the handler escalates — a signal that the simulation's conservation law has been breached.

### Gravity Decay

The `GravityDecayHandler` applies exponential decay:

$$g_{t+1} = g_t \cdot \alpha$$

where α is the decay factor (e.g., 0.95 means 5% decay per tick). Over n ticks:

$$g_n = g_0 \cdot \alpha^n \to 0$$

This models perturbations in room gravity decaying toward equilibrium.

---

## License

MIT or Apache-2.0 (at your option).
