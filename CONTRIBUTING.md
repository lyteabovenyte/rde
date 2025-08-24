# Contributing to RDE

Thank you for your interest in contributing to RDE! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites

- **Rust 1.70+** - [Install Rust](https://rustup.rs/)
- **Docker** - [Install Docker](https://docs.docker.com/get-docker/)
- **Git** - Version control

### Development Setup

1. **Fork and Clone**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/rde.git
   cd rde
   ```

2. **Build and Test**:
   ```bash
   # Build all components
   cargo build

   # Run tests
   cargo test

   # Check code quality
   cargo clippy
   cargo fmt
   ```

3. **Start Infrastructure**:
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

## 📝 Development Guidelines

### Code Style

- **Rust Standards**: Follow standard Rust conventions
- **Documentation**: All public APIs must have doc comments
- **Testing**: Write tests for new functionality
- **Error Handling**: Use `anyhow::Result` for error propagation

### Documentation Standards

```rust
/// Brief description of the function
///
/// Longer description with usage examples and important notes.
///
/// # Arguments
/// * `param1` - Description of parameter
/// * `param2` - Description of parameter
///
/// # Returns
/// Description of return value
///
/// # Errors
/// Description of error conditions
///
/// # Example
/// ```rust
/// let result = my_function(arg1, arg2)?;
/// ```
pub fn my_function(param1: Type1, param2: Type2) -> Result<ReturnType> {
    // Implementation
}
```

### Testing Guidelines

- **Unit Tests**: Test individual functions and methods
- **Integration Tests**: Test operator interactions
- **End-to-End Tests**: Test complete pipeline scenarios
- **Performance Tests**: Benchmark critical paths

Example test structure:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_name() {
        // Arrange
        let input = create_test_input();
        
        // Act
        let result = function_under_test(input).unwrap();
        
        // Assert
        assert_eq!(result.field, expected_value);
    }

    #[tokio::test]
    async fn test_async_function() {
        // Test async functionality
    }
}
```

## 🔧 Types of Contributions

### 1. Bug Reports

When reporting bugs, please include:

- **Description**: Clear description of the issue
- **Reproduction Steps**: Step-by-step instructions
- **Expected vs Actual**: What should happen vs what happens
- **Environment**: OS, Rust version, configuration
- **Logs**: Relevant error messages or logs

Use this template:
```markdown
## Bug Description
Brief description of the bug.

## Steps to Reproduce
1. Step one
2. Step two
3. Step three

## Expected Behavior
What should happen.

## Actual Behavior
What actually happens.

## Environment
- OS: [e.g., Ubuntu 22.04]
- Rust Version: [e.g., 1.75.0]
- RDE Version: [e.g., 2.0.0]

## Additional Context
Any other relevant information.
```

### 2. Feature Requests

When requesting features, please include:

- **Problem Statement**: What problem does this solve?
- **Proposed Solution**: How should it work?
- **Alternatives**: Other solutions considered
- **Use Cases**: Real-world scenarios

### 3. Code Contributions

#### New Operators

To add a new source, transform, or sink:

1. **Create the Implementation**:
   ```rust
   // In appropriate module
   pub struct MyNewOperator {
       id: String,
       schema: SchemaRef,
       config: MyConfig,
   }

   #[async_trait]
   impl Source for MyNewOperator { // or Transform/Sink
       async fn run(&mut self, tx: BatchTx, cancel: CancellationToken) -> Result<()> {
           // Implementation
       }
   }
   ```

2. **Add Configuration**:
   ```rust
   // In rde-core/src/lib.rs
   #[derive(Debug, Clone, Serialize, Deserialize)]
   pub struct MyOperatorSpec {
       pub id: String,
       pub my_config_field: String,
       // Other fields
   }
   ```

3. **Update Builder Logic**:
   ```rust
   // In builder code
   SourceSpec::MyNewSource(spec) => {
       Box::new(MyNewOperator::new(spec)?)
   }
   ```

4. **Add Tests**:
   ```rust
   #[tokio::test]
   async fn test_my_new_operator() {
       // Test implementation
   }
   ```

5. **Update Documentation**:
   - Add to configuration reference
   - Include usage examples
   - Update README if significant

#### Performance Improvements

For performance contributions:

- **Benchmark Before/After**: Use `cargo bench`
- **Profile Hot Paths**: Use `perf` or similar tools
- **Document Changes**: Explain the optimization
- **Avoid Breaking Changes**: Maintain API compatibility

### 4. Documentation

Documentation improvements are always welcome:

- **API Documentation**: Improve doc comments
- **User Guides**: Add tutorials and examples
- **Configuration**: Update configuration reference
- **Architecture**: Explain system design

## 🔄 Development Workflow

### Branch Strategy

- **main**: Stable releases
- **develop**: Integration branch for features
- **feature/**: Feature development branches
- **bugfix/**: Bug fix branches

### Pull Request Process

1. **Create Feature Branch**:
   ```bash
   git checkout -b feature/my-new-feature
   ```

2. **Make Changes**:
   - Write code following guidelines
   - Add tests
   - Update documentation

3. **Test Thoroughly**:
   ```bash
   cargo test
   cargo clippy
   cargo fmt
   ./scripts/test-end-to-end.sh
   ```

4. **Commit Changes**:
   ```bash
   git commit -m "feat: add new operator for X

   - Implements Y functionality
   - Adds configuration options for Z
   - Includes comprehensive tests"
   ```

5. **Push and Create PR**:
   ```bash
   git push origin feature/my-new-feature
   ```
   Then create a pull request on GitHub.

### Commit Message Format

Use conventional commits:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance tasks

Examples:
```
feat(kafka): add schema registry support
fix(iceberg): handle concurrent schema evolution
docs(config): update pipeline configuration examples
```

### Code Review Process

All contributions require review:

1. **Automated Checks**: CI/CD runs tests and checks
2. **Peer Review**: At least one maintainer review
3. **Quality Gates**: Code coverage and performance checks
4. **Documentation Review**: Ensure docs are updated

## 🧪 Testing

### Running Tests

```bash
# Unit tests
cargo test

# Integration tests
cargo test --test integration

# End-to-end tests
./scripts/test-end-to-end.sh

# Performance tests
cargo bench
```

### Writing Tests

#### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn test_schema_evolution() {
        let manager = DynamicSchemaManager::new();
        let schema = manager.infer_schema(&json_data).unwrap();
        assert_eq!(schema.fields().len(), 3);
    }
}
```

#### Integration Tests
```rust
// tests/integration_test.rs
use rde_core::*;
use rde_io::*;

#[tokio::test]
async fn test_kafka_to_iceberg_pipeline() {
    // Test complete pipeline
}
```

### Test Data

- Use the `data/json-samples/` directory for test data
- Create minimal, focused test cases
- Include edge cases and error conditions

## 📚 Documentation

### API Documentation

```bash
# Generate docs
cargo doc --open

# Check doc warnings
cargo doc --document-private-items
```

### User Documentation

Located in `docs/`:
- `getting-started.md` - Tutorial for new users
- `configuration.md` - Complete configuration reference
- `architecture.md` - System design and internals

## 🚀 Release Process

### Version Management

RDE uses semantic versioning:
- **Major**: Breaking changes
- **Minor**: New features, backward compatible
- **Patch**: Bug fixes

### Release Checklist

1. Update version numbers
2. Update CHANGELOG.md
3. Test release candidate
4. Create GitHub release
5. Publish documentation updates

## 🎯 Contribution Areas

We especially welcome contributions in:

### High Priority
- **New Data Sources**: Database connectors, cloud services
- **Advanced Transforms**: ML features, complex aggregations
- **Performance**: Optimization and scaling improvements
- **Monitoring**: Metrics and observability features

### Medium Priority
- **Documentation**: User guides and examples
- **Testing**: Coverage improvements and test utilities
- **DevOps**: CI/CD and deployment improvements
- **Examples**: Real-world use case demonstrations

### Low Priority
- **Refactoring**: Code quality improvements
- **Tooling**: Development experience enhancements
- **Dependencies**: Library updates and maintenance

## 🏆 Recognition

Contributors are recognized in:
- **CONTRIBUTORS.md**: All contributors listed
- **Release Notes**: Major contributions highlighted
- **GitHub**: Contributor statistics and badges

## 📞 Getting Help

- **Documentation**: Check `docs/` directory first
- **Discussions**: [GitHub Discussions](https://github.com/lyteabovenyte/rde/discussions)
- **Issues**: [GitHub Issues](https://github.com/lyteabovenyte/rde/issues)
- **Discord**: [RDE Community Discord](https://discord.gg/rde-community)

## 📄 License

By contributing to RDE, you agree that your contributions will be licensed under the same MIT license that covers the project.

---

Thank you for contributing to RDE! Your efforts help make data engineering more accessible and powerful for everyone. 🎉
