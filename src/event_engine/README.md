# Event Engine (`src/event_engine/`)

This is a Sectional Blueprint for the Event Engine wing of the Cathedral.

## Purpose

The Event Engine is responsible for processing various types of asynchronous events that occur within the system. It provides a decoupled way to handle side effects, notifications, and other event-driven logic.

## Structure

*   **`handler/`**: Contains the core event handling logic, including event definitions, processors, and the main event processing function. (See `src/event_engine/handler/README.md`)

## Architectural Principles

This module adheres to the Grand Unifying Manifesto, emphasizing:
*   **Hierarchical Decomposition**: Clearly separated from other core domains.
*   **Internal Composition**: The `handler` submodule will further delineate roles (Mind, Soul, etc.).
*   **True Names**: Module and file names are chosen for clarity.

## Future Considerations

*   Event sourcing capabilities.
*   Integration with a persistent message queue.
*   Advanced event routing and filtering.
