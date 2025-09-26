"""
Unit tests for the main optimization application (app_sequential_pydantic.py).
"""
import pytest
import json
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import the main function from the application
# Note: This assumes the main function is importable from app_sequential_pydantic
# You may need to adjust the import based on the actual structure

from src.models.optimizer import SequentialOptimizer
from src.models.schema import Schema


class TestMainOptimizationApp:
    """Test cases for the main optimization application."""

    @pytest.fixture
    def sample_input_config(self):
        """Sample input configuration for testing."""
        return {
            "project_arn": "arn:aws:bedrock-data-automation:us-west-2:123456789012:project/test-project",
            "blueprint_arn": "arn:aws:bedrock-data-automation:us-west-2:123456789012:blueprint/test-blueprint",
            "blueprint_ver": "1",
            "blueprint_stage": "DEVELOPMENT",
            "input_bucket": "s3://test-input-bucket/",
            "output_bucket": "s3://test-output-bucket/",
            "document_name": "test_document.pdf",
            "document_s3_uri": "s3://test-bucket/test_document.pdf",
            "threshold": 0.8,
            "max_iterations": 3,
            "model": "anthropic.claude-3-sonnet-20240229-v1:0",
            "use_document_strategy": True,
            "clean_logs": False,
            "expected_outputs": {
                "invoice_number": "INV-12345",
                "total_amount": "$1,234.56",
                "date": "2024-01-15"
            }
        }

    @patch('builtins.open', create=True)
    @patch('json.load')
    def test_load_configuration_success(self, mock_json_load, mock_open, sample_input_config):
        """Test successful configuration loading."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_json_load.return_value = sample_input_config

        # This would be the actual function call in the main app
        # Adjust based on actual implementation
        config_file = "input_0.json"
        
        # Simulate loading configuration
        with open(config_file, 'r') as f:
            config = json.load(f)

        assert config == sample_input_config
        mock_open.assert_called_once_with(config_file, 'r')

    @patch('builtins.open', create=True)
    def test_load_configuration_file_not_found(self, mock_open):
        """Test configuration loading when file not found."""
        mock_open.side_effect = FileNotFoundError("Configuration file not found")

        config_file = "nonexistent_input.json"

        with pytest.raises(FileNotFoundError):
            with open(config_file, 'r') as f:
                json.load(f)

    @patch('builtins.open', create=True)
    @patch('json.load')
    def test_load_configuration_invalid_json(self, mock_json_load, mock_open):
        """Test configuration loading with invalid JSON."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_json_load.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        config_file = "invalid_input.json"

        with pytest.raises(json.JSONDecodeError):
            with open(config_file, 'r') as f:
                json.load(f)

    def test_configuration_validation(self, sample_input_config):
        """Test configuration validation logic."""
        # This would be a validation function in the main app
        def validate_config(config):
            required_fields = [
                'project_arn', 'blueprint_arn', 'blueprint_ver', 
                'blueprint_stage', 'threshold', 'max_iterations'
            ]
            
            for field in required_fields:
                if field not in config or not config[field]:
                    return False, f"Missing required field: {field}"
            
            if not (0.0 <= config['threshold'] <= 1.0):
                return False, "Threshold must be between 0.0 and 1.0"
            
            if config['max_iterations'] <= 0:
                return False, "Max iterations must be positive"
            
            return True, "Valid configuration"

        # Test valid configuration
        is_valid, message = validate_config(sample_input_config)
        assert is_valid is True
        assert message == "Valid configuration"

        # Test invalid threshold
        invalid_config = sample_input_config.copy()
        invalid_config['threshold'] = 1.5
        is_valid, message = validate_config(invalid_config)
        assert is_valid is False
        assert "Threshold must be between" in message

        # Test missing field
        incomplete_config = sample_input_config.copy()
        del incomplete_config['project_arn']
        is_valid, message = validate_config(incomplete_config)
        assert is_valid is False
        assert "Missing required field: project_arn" in message

    @patch('time.time')
    def safe_operation(operation, *args, **kwargs):
        try:
            return operation(*args, **kwargs), None
        except Exception as e:
            return None, str(e)

        # Test successful operation
        def successful_op():
            return "success"

        result, error = safe_operation(successful_op)
        assert result == "success"
        assert error is None

        # Test failing operation
        def failing_op():
            raise ValueError("Operation failed")

        result, error = safe_operation(failing_op)
        assert result is None
        assert error == "Operation failed"
        filename = generate_output_filename("optimized_schema", "20240101_120000")
        assert filename == "optimized_schema_20240101_120000.json"

        # Test with auto timestamp
        with patch('src.util.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.strftime.return_value = "20240101_120000"
            mock_datetime.now.return_value = mock_now
            
            filename = generate_output_filename("optimized_schema")
            assert filename == "optimized_schema_20240101_120000.json"


class TestNestedBlueprintSupport:
    """Test cases for nested blueprint support in the optimizer."""

    @pytest.fixture
    def nested_schema_data(self):
        """Sample nested schema for testing."""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Test nested invoice schema",
            "class": "invoice",
            "type": "object",
            "properties": {
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "inferenceType": "explicit",
                            "instruction": "Extract customer name"
                        },
                        "address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                    "type": "string",
                                    "inferenceType": "explicit",
                                    "instruction": "Extract street address"
                                }
                            }
                        }
                    }
                },
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "price": {
                                "type": "number",
                                "inferenceType": "explicit",
                                "instruction": "Extract item price"
                            }
                        }
                    }
                }
            }
        }

    @pytest.fixture
    def flat_schema_data(self):
        """Sample flat schema for testing."""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Test flat invoice schema",
            "class": "invoice",
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "inferenceType": "explicit",
                    "instruction": "Extract invoice number"
                },
                "total_amount": {
                    "type": "number",
                    "inferenceType": "explicit",
                    "instruction": "Extract total amount"
                }
            }
        }

    def test_nested_schema_detection(self, nested_schema_data, flat_schema_data):
        """Test that nested schemas are correctly detected."""
        # Test nested schema detection
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(nested_schema_data, f)
            nested_schema_path = f.name

        try:
            nested_schema = Schema.from_file(nested_schema_path)
            assert nested_schema.is_nested() is True
        finally:
            os.unlink(nested_schema_path)

        # Test flat schema detection
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(flat_schema_data, f)
            flat_schema_path = f.name

        try:
            flat_schema = Schema.from_file(flat_schema_path)
            assert flat_schema.is_nested() is False
        finally:
            os.unlink(flat_schema_path)

    def test_nested_schema_flattening(self, nested_schema_data):
        """Test that nested schemas are correctly flattened."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(nested_schema_data, f)
            schema_path = f.name

        try:
            schema = Schema.from_file(schema_path)
            flattened_schema, path_mapping = schema.flatten_for_optimization()

            # Check that flattened schema has dot-notation field names
            expected_fields = ["customer.name", "customer.address.street", "items[*].price"]
            for field in expected_fields:
                assert field in flattened_schema.properties

            # Check that path mapping is created
            assert len(path_mapping) == len(expected_fields)

            # Verify flattened properties maintain their instruction content
            assert flattened_schema.properties["customer.name"].instruction == "Extract customer name"
            assert flattened_schema.properties["items[*].price"].instruction == "Extract item price"

        finally:
            os.unlink(schema_path)

    def test_nested_schema_unflattening(self, nested_schema_data):
        """Test that flattened schemas are correctly reconstructed."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(nested_schema_data, f)
            schema_path = f.name

        try:
            original_schema = Schema.from_file(schema_path)
            flattened_schema, path_mapping = original_schema.flatten_for_optimization()

            # Modify instructions in flattened schema
            flattened_schema.properties["customer.name"].instruction = "Updated customer name instruction"
            flattened_schema.properties["items[*].price"].instruction = "Updated price instruction"

            # Reconstruct nested schema
            reconstructed_schema = original_schema.unflatten_from_optimization(
                flattened_schema, path_mapping
            )

            # Verify structure is reconstructed correctly
            assert "customer" in reconstructed_schema.properties
            assert "items" in reconstructed_schema.properties

            # Verify updated instructions are preserved in nested structure
            customer_props = reconstructed_schema.properties["customer"]["properties"]
            assert customer_props["name"]["instruction"] == "Updated customer name instruction"

            items_props = reconstructed_schema.properties["items"]["items"]["properties"]
            assert items_props["price"]["instruction"] == "Updated price instruction"

        finally:
            os.unlink(schema_path)

    @patch('src.models.optimizer.BDAClient')
    @patch('src.models.optimizer.BDAConfig')
    def test_optimizer_handles_nested_blueprint(self, mock_config_class, mock_bda_client_class, 
                                              nested_schema_data):
        """Test that SequentialOptimizer correctly handles nested blueprints."""
        # Create temporary schema file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(nested_schema_data, f)
            schema_path = f.name

        try:
            # Mock configuration
            mock_config = Mock()
            mock_config.inputs = [
                Mock(field_name="customer.name"),
                Mock(field_name="customer.address.street"),
                Mock(field_name="items[*].price")
            ]
            mock_config_class.from_file.return_value = mock_config

            # Mock BDA client
            mock_bda_client = Mock()
            mock_bda_client_class.from_config.return_value = mock_bda_client

            # Mock the get_blueprint_schema_to_file method to create the schema file
            def mock_get_schema(path):
                with open(path, 'w') as f:
                    json.dump(nested_schema_data, f)

            mock_bda_client.get_blueprint_schema_to_file.side_effect = mock_get_schema

            # Create optimizer
            with patch('os.makedirs'):
                optimizer = SequentialOptimizer.from_config_file(
                    config_file="test_config.json",
                    threshold=0.8,
                    use_doc=False,
                    use_template=True,
                    model_choice="test-model",
                    max_iterations=3
                )

            # Verify nested blueprint is detected and handled
            assert optimizer.is_nested_blueprint is True
            assert len(optimizer.path_mapping) > 0
            assert optimizer.original_schema is not None

            # Verify flattened schema is used for optimization
            expected_fields = ["customer.name", "customer.address.street", "items[*].price"]
            for field in expected_fields:
                assert field in optimizer.schema.properties

        finally:
            os.unlink(schema_path)

    @patch('src.models.optimizer.BDAClient')
    @patch('src.models.optimizer.BDAConfig')
    def test_optimizer_backward_compatibility_flat_blueprint(self, mock_config_class, mock_bda_client_class, 
                                                           flat_schema_data):
        """Test that SequentialOptimizer maintains backward compatibility with flat blueprints."""
        # Create temporary schema file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(flat_schema_data, f)
            schema_path = f.name

        try:
            # Mock configuration
            mock_config = Mock()
            mock_config.inputs = [
                Mock(field_name="invoice_number"),
                Mock(field_name="total_amount")
            ]
            mock_config_class.from_file.return_value = mock_config

            # Mock BDA client
            mock_bda_client = Mock()
            mock_bda_client_class.from_config.return_value = mock_bda_client

            # Mock the get_blueprint_schema_to_file method to create the schema file
            def mock_get_schema(path):
                with open(path, 'w') as f:
                    json.dump(flat_schema_data, f)

            mock_bda_client.get_blueprint_schema_to_file.side_effect = mock_get_schema

            # Create optimizer
            with patch('os.makedirs'):
                optimizer = SequentialOptimizer.from_config_file(
                    config_file="test_config.json",
                    threshold=0.8,
                    use_doc=False,
                    use_template=True,
                    model_choice="test-model",
                    max_iterations=3
                )

            # Verify flat blueprint is handled correctly
            assert optimizer.is_nested_blueprint is False
            assert len(optimizer.path_mapping) == 0
            assert optimizer.original_schema is not None

            # Verify schema remains unchanged for flat blueprints
            expected_fields = ["invoice_number", "total_amount"]
            for field in expected_fields:
                assert field in optimizer.schema.properties

        finally:
            os.unlink(schema_path)
