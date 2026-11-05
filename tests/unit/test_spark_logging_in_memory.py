"""
Unit tests for in-memory logging feature in spark_logging.py
"""

import json

import pytest

# Package is installed via pip install -e . — no sys.path manipulation needed
from spark_quick_start.spark_logging import (
    LogSession,
    get_logger,
    get_logs,
    get_logs_json,
    init_logs,
)


class TestInMemoryLogging:
    """Tests for in-memory logging functionality."""
    
    def setup_method(self):
        """Reset global state before each test."""
        import spark_quick_start.spark_logging as fl
        fl._CURRENT = None
    
    def test_init_in_memory_mode(self):
        """Test initialization in in-memory mode."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True
        )
        
        assert session.in_memory is True
        assert session._full_payload is None
        assert not session._finalized
    
    def test_logging_in_memory(self):
        """Test that logs are captured in memory."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            capture_stdout=True,
            in_memory=True
        ).start()
        
        log = session.logger
        log.info("Test message 1")
        log.warning("Test message 2")
        log.error("Test message 3")
        
        # Finalize and get logs
        logs = session.get_logs()
        
        assert logs['run_id'] == session.run_id
        assert logs['success'] is True
        assert logs['python_log_count'] >= 3
        
        # Check log messages
        messages = [entry['message'] for entry in logs['python_logs']]
        assert "Test message 1" in messages
        assert "Test message 2" in messages
        assert "Test message 3" in messages
    
    def test_get_logs_json(self):
        """Test retrieving logs as JSON string."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True
        ).start()
        
        log = session.logger
        log.info("JSON test message")
        
        logs_json = session.get_logs_json()
        
        # Should be valid JSON
        logs = json.loads(logs_json)
        assert 'run_id' in logs
        assert 'python_logs' in logs
        assert logs['python_log_count'] > 0
    
    def test_get_logs_dict(self):
        """Test retrieving logs as dictionary."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True
        ).start()
        
        log = session.logger
        log.info("Dict test message")
        
        logs = session.get_logs()
        
        # Should be a dictionary
        assert isinstance(logs, dict)
        assert 'run_id' in logs
        assert 'python_logs' in logs
        assert logs['python_log_count'] > 0
    
    def test_correlation_id_in_memory(self):
        """Test that correlation ID is included in logs."""
        correlation_id = "test-correlation-123"
        
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True,
            correlation_id=correlation_id
        ).start()
        
        log = session.logger
        log.info("Test with correlation")
        
        logs = session.get_logs()
        
        assert logs['meta']['correlation_id'] == correlation_id
        # Check that correlation_id is in log entries
        assert logs['python_logs'][0].get('correlation_id') == correlation_id
    
    def test_context_in_memory(self):
        """Test that context is included in logs."""
        context = {"environment": "test", "batch_id": "B123"}
        
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True,
            context=context
        ).start()
        
        log = session.logger
        log.info("Test with context")
        
        logs = session.get_logs()
        
        assert logs['meta']['context'] == context
    
    def test_stdout_capture_in_memory(self):
        """Test that stdout is captured in in-memory mode."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            capture_stdout=True,
            in_memory=True
        ).start()
        
        print("Test stdout message")
        
        logs = session.get_logs()
        
        assert "Test stdout message" in logs['stdout_tail']
    
    def test_error_capture_in_memory(self):
        """Test that errors are captured in in-memory mode."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True
        ).start()
        
        log = session.logger
        log.error("Error message")
        
        # Note an error
        session.note_error("TestError", "Test error message", "Test traceback")
        
        logs = session.get_logs()
        
        assert logs['error'] is not None
        assert logs['error']['type'] == "TestError"
        assert logs['error']['message'] == "Test error message"
    
    def test_finalize_idempotent_in_memory(self):
        """Test that finalize is idempotent in in-memory mode."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True
        ).start()
        
        log = session.logger
        log.info("Test message")
        
        # First finalize
        logs1 = session.get_logs()
        run_id1 = logs1['run_id']
        
        # Second finalize should return same data
        logs2 = session.get_logs()
        run_id2 = logs2['run_id']
        
        assert run_id1 == run_id2
        assert logs1['python_log_count'] == logs2['python_log_count']
    
    def test_global_functions_in_memory(self):
        """Test global convenience functions with in-memory mode."""
        session = init_logs(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True
        )
        
        log = get_logger()
        log.info("Global function test")
        
        logs_json = get_logs_json()
        logs = get_logs()
        
        assert isinstance(logs_json, str)
        assert isinstance(logs, dict)
        assert logs['python_log_count'] > 0
    
    def test_context_manager_in_memory(self):
        """Test using LogSession as context manager in in-memory mode."""
        with LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True
        ) as session:
            log = session.logger
            log.info("Context manager test")
        
        # After exiting context, session should be finalized
        logs = session.get_logs()
        assert logs['python_log_count'] > 0
        assert session._finalized is True
    
    def test_no_persistence_in_memory(self):
        """Test that in-memory mode doesn't attempt to persist to lakehouse."""
        session = LogSession(
            name_prefix="test",
            level="INFO",
            capture_spark=False,
            in_memory=True,
            lakehouse_dir="Files/Logs"  # Should be ignored
        ).start()
        
        log = session.logger
        log.info("No persistence test")
        
        # This should not raise an error even though mssparkutils is not available
        logs = session.get_logs()
        
        # Should have full payload, not file paths
        assert 'python_logs' in logs
        assert 'full_python_logs_path' not in logs
    
    def test_log_levels_in_memory(self):
        """Test different log levels in in-memory mode."""
        session = LogSession(
            name_prefix="test",
            level="DEBUG",
            capture_spark=False,
            in_memory=True
        ).start()
        
        log = session.logger
        log.debug("Debug message")
        log.info("Info message")
        log.warning("Warning message")
        log.error("Error message")
        
        logs = session.get_logs()
        
        levels = [entry['levelname'] for entry in logs['python_logs'] if 'message' in entry['message'].lower()]
        assert 'DEBUG' in levels
        assert 'INFO' in levels
        assert 'WARNING' in levels
        assert 'ERROR' in levels


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
