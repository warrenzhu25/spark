# Fetch Busy Load-Aware System

## Overview

The Fetch Busy system is a load-aware mechanism designed to prevent network overload in Apache Spark's shuffle and block transfer operations. It provides backpressure control by rejecting new fetch requests when the system is under high load.

## Architecture

### Core Components

1. **ChunkFetchRequestHandler** - Foundation component that checks fetch busy status before processing requests
2. **TransportConf** - Configuration management for thresholds and timeouts
3. **BlockManager Integration** - Handles fetch busy exceptions during block migration and decommissioning
4. **Upload Stream Handling** - Manages RPC failures when streams are rejected due to busy conditions

### Configuration Parameters

- `fetchBusyHighWatermark` - Upper threshold for triggering fetch busy state
- `fetchBusyLowWatermark` - Lower threshold for exiting fetch busy state  
- `fetchBusyCoolDownTime` - Cooldown period to prevent oscillation between states

### Load Balancing Features

- **Migration Peer Support** - Allows BlockManager to find alternative peers during busy periods
- **Graceful Degradation** - Properly handles rejections without causing client hangs
- **Exception Handling** - Comprehensive error handling throughout the stack

## Implementation Timeline

The system was implemented in phases:

1. **Foundation** (Feb 2024) - Core fetch busy checker and configuration
2. **Error Handling** (Feb 2024) - RPC failure handling for rejected streams  
3. **Integration** (Feb 2024) - Connected components together
4. **Block Manager** (Jul 2023) - Added migration support and exception handling
5. **Refinement** (Oct 2023) - Added configurable watermarks and cooldown

## Usage

The system operates automatically once configured. When network load exceeds the high watermark, new fetch requests are rejected. The system returns to normal operation when load drops below the low watermark after the cooldown period.

## Benefits

- Prevents network saturation during high load periods
- Maintains system stability during peak traffic
- Provides graceful degradation rather than system failure
- Allows for load balancing through peer migration