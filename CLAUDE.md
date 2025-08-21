# Claude Code Development Guidelines for Spark Shuffle Load Balancing

## Code Style Requirements

### Formatting Rules
- **No trailing whitespace**: Never leave trailing whitespace at the end of lines
- **No empty lines with whitespace**: Empty lines should be completely empty with no spaces or tabs
- **Consistent indentation**: Use 2 spaces for Scala code indentation
- **Line endings**: Use Unix line endings (LF only)
- **File endings**: All files must end with a newline character

### Scala Style Guidelines
- Follow existing Spark code conventions
- Use meaningful variable and method names
- Keep line length under 100 characters when possible
- Place opening braces on the same line as the declaration
- Use spaces around operators and after commas

### Test Requirements
- All new functionality must have corresponding unit tests
- Tests should cover both positive and negative cases
- Use descriptive test method names
- Follow the existing test structure and patterns

## Development Process

### Commit Strategy
- Each phase should be a separate commit
- All tests must pass before committing
- Commits should be atomic and focused
- Use clear, descriptive commit messages

### Testing Requirements
- Run tests before each commit: `./build/sbt "core/testOnly <TestSuite>"`
- Ensure all style checks pass
- Fix any scalastyle violations before proceeding
- Test both unit tests and integration tests where applicable

## Current Project: Shuffle Load Balancing

### Implementation Phases
1. **Phase 1**: Communication infrastructure (messages, heartbeat extensions)
2. **Phase 2**: Load metrics collection and reporting
3. **Phase 3**: Driver-side load balancing logic
4. **Phase 4**: Executor-side fetch adaptation
5. **Phase 5**: Feedback loop optimization
6. **Phase 6**: Comprehensive testing and validation

### Key Files Being Modified
- `BlockManagerMessages.scala`: New message types for load balancing
- `HeartbeatReceiver.scala`: Extended heartbeat with shuffle metrics
- `ShuffleBlockFetcherIterator.scala`: Load-aware fetch scheduling
- Test files: Comprehensive test coverage for all new functionality