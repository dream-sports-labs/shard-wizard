# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Configurable Router Strategies**: Added support for configurable routing strategies through `routerType` configuration
  - `MODULO`: Perfect distribution for numeric keys with O(1) performance
  - `CONSISTENT`: Consistent hashing for minimal data movement during scaling
- **ShardRouterFactory**: Factory class for creating router instances based on configuration
- **Enhanced Test Coverage**: Added comprehensive distribution tests for both routing strategies

### Changed
- **AbstractDaoFactory**: Now uses configuration-driven router selection instead of hardcoded ModuloRouter
- **ShardManagerConfig**: Extended with `routerType` field supporting MODULO (default) and CONSISTENT options

### Fixed
- **Test Infrastructure** ([#12](https://github.com/dream-sports-labs/shard-wizard/pull/12)): Comprehensive test fixes and refactoring
  - Fixed DynamoDB shard manager configuration and test cases
  - Resolved PostgreSQL configuration issues in test environments
  - Corrected syntax errors and removed unused test files
  - Standardized shard master configuration across different database types
  - Improved test case organization and reduced redundant test files
  - Enhanced configuration injection for test scenarios
  - Updated documentation to reflect test improvements

### Removed
- **Unused Files**: Cleaned up obsolete test files and unused table mappings
- **Redundant Test Cases**: Consolidated and streamlined test suite for better maintainability

### Backward Compatibility
- All existing code continues to work unchanged with MODULO routing as the default
- No breaking changes to existing APIs or configuration files

## [Previous Versions]

Previous version history to be documented...
