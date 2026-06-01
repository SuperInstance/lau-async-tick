# lau-async-tick

> THE async tick engine — non-blocking tick processing with priority queues

## What This Does

THE async tick engine — non-blocking tick processing with priority queues. Part of the PLATO/LAU ecosystem — a mathematically rigorous framework for building educational agents that learn, teach, and evolve.

## The Key Idea

This crate implements the core abstractions needed for its domain, with a focus on correctness, composability, and conservation guarantees. Every public type is serializable (serde), every algorithm is tested, and every invariant is verified.

## Install

```bash
cargo add lau-async-tick
```

## Quick Start

See the API Reference below for complete usage. Key entry points:

```rust
use lau_async_tick::*;
// See types and methods below for complete usage
```

## API Reference

```rust
pub enum TickPriority 
pub enum TickTaskType 
    pub fn is_urgent(&self) -> bool 
pub struct TickTask 
    pub fn new(task_type: TickTaskType) -> Self 
    pub fn with_data(mut self, k: &str, v: &str) -> Self 
    pub fn with_priority(mut self, p: TickPriority) -> Self 
pub enum TickError 
pub enum TickAction 
    pub fn describe(&self) -> String 
pub struct RoomState 
pub struct EnsignState 
pub struct TickContext 
pub trait TickHandler: Send + Sync 
pub struct TickResult 
    pub fn summary(&self) -> String 
pub struct TickEngineStats 
pub struct TickEngine 
    pub fn new(interval_ms: u64) -> Self 
    pub fn register_handler(&mut self, task_type: TickTaskType, handler: Box<dyn TickHandler>) 
    pub fn enqueue(&mut self, task: TickTask) 
    pub fn context_mut(&mut self) -> &mut TickContext 
    pub fn set_context(&mut self, ctx: TickContext) 
    pub fn tick(&mut self) -> TickResult 
    pub fn run(&mut self, max_ticks: Option<u64>) -> Vec<TickResult> 
    pub fn drain(&mut self) -> Vec<TickResult> 
    pub fn stats(&self) -> TickEngineStats 
    pub fn queue_len(&self) -> usize 
pub struct DeadbandCheckHandler 
    pub fn new(threshold: f64) -> Self 
pub struct GravityDecayHandler 
    pub fn new(decay_factor: f64) -> Self 
pub struct ConservationCheckHandler 
    pub fn new(warn_threshold: f64) -> Self 
```

## How It Works

Read the source in `src/` for full implementation details. All algorithms are documented with inline comments explaining the mathematical foundations.

## The Math

This crate implements formal mathematical constructs. See the source documentation for theorem statements and proofs of correctness.

## Testing

**55 tests** covering construction, serialization, correctness properties, edge cases, and composability with other lau-* crates.

## License

MIT
