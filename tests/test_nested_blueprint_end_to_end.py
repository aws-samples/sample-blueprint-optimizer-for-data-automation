"""
End-to-end tests for nested blueprint processing workflow.
Tests the complete workflow: import → flatten → optimize → unflatten → export
"""
import pytest
import json
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.models.optimizer import SequentialOptimizer
from src.models.schema import Schema
from src.services.schema_converter import SchemaFlattener, SchemaUnflattener


class TestNestedBlueprintEndToEnd:
    """End-to-end tests for nested blueprint processing."""

    @pytest.fixture
    def sample_nested_blueprint(self):
        """Sample nested blueprint from BDA for testing."""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Invoice document with nested customer and line items",
            "class": "invoice",
            "type": "object",
            "definitions": {},
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "inferenceType": "explicit",
                    "instruction": "Extract the invoice number"
                },
                "date": {
                    "type": "string",
                    "inferenceType": "explicit", 
                    "instruction": "Extract the invoice date"
                },
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "inferenceType": "explicit",
                            "instruction": "Extract customer name"
                        },
                        "email": {
                            "type": "string",
                            "inferenceType": "explicit",
                            "instruction": "Extract customer email address"
                        },
                        "address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                    "type": "string",
                                    "inferenceType": "explicit",
                                    "instruction": "Extract street address"
                                },
                                "city": {
                                    "type": "string",
                                    "inferenceType": "explicit",
                                    "instruction": "Extract city name"
                                },
                                "state": {
                                    "type": "string",
                                    "inferenceType": "explicit",
                                    "instruction": "Extract state or province"
                                },
                                "zip_code": {
                                    "type": "string",
                                    "inferenceType": "explicit",
                                    "instruction": "Extract postal code"
                                }
                            }
                        }
                    }
                },
                "line_items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "description": {
                                "type": "string",
                                "inferenceType": "explicit",
                                "instruction": "Extract item description"
                            },
                            "quantity": {
                                "type": "number",
                                "inferenceType": "explicit",
                                "instruction": "Extract quantity"
                            },
                            "unit_price": {
                                "type": "number",
                                "inferenceType": "explicit",
                                "instruction": "Extract unit price"
                            },
                            "total_price": {
                                "type": "number",
                                "inferenceType": "explicit",
                                "instruction": "Extract total price for this line item"
                            }
                        }
                    }
                },
                "subtotal": {
                    "type": "number",
                    "inferenceType": "explicit",
                    "instruction": "Extract subtotal amount"
                },
                "tax": {
                    "type": "number",
                    "inferenceType": "explicit",
                    "instruction": "Extract tax amount"
                },
                "total": {
                    "type": "number",
                    "inferenceType": "explicit",
                    "instruction": "Extract total amount"
                }
            }
        }

    @pytest.fixture
    def sample_config_data(self):
        """Sample configuration data for testing."""
        return {
            "project_arn": "arn:aws:bedrock-data-automation:us-west-2:123456789012:project/test-project",
            "blueprint_arn": "arn:aws:bedrock-data-automation:us-west-2:123456789012:blueprint/test-blueprint",
            "blueprint_ver": "1",
            "blueprint_stage": "DEVELOPMENT",
            "input_bucket": "s3://test-input-bucket/",
            "output_bucket": "s3://test-output-bucket/",
            "document_name": "test_invoice.pdf",
            "document_s3_uri": "s3://test-bucket/test_invoice.pdf",
            "threshold": 0.8,
            "max_iterations": 3,
            "model": "anthropic.claude-3-sonnet-20240229-v1:0",
            "use_document_strategy": True,
            "clean_logs": False,
            "inputs": [
                {
                    "field_name": "invoice_number",
                    "instruction": "Extract the invoice number",
                    "expected_output": "INV-12345",
                    "data_point_in_document": "Invoice number appears at the top"
                },
                {
                    "field_name": "customer.name",
                    "instruction": "Extract customer name",
                    "expected_output": "John Smith",
                    "data_point_in_document": "Customer name in billing section"
                },
                {
                    "field_name": "customer.email",
                    "instruction": "Extract customer email address",
                    "expected_output": "john.smith@example.com",
                    "data_point_in_document": "Email in customer details"
                },
                {
                    "field_name": "customer.address.street",
                    "instruction": "Extract street address",
                    "expected_output": "123 Main St",
                    "data_point_in_document": "Street address in billing section"
                },
                {
                    "field_name": "customer.address.city",
                    "instruction": "Extract city name",
                    "expected_output": "Anytown",
                    "data_point_in_document": "City in billing address"
                },
                {
                    "field_name": "line_items[*].description",
                    "instruction": "Extract item description",
                    "expected_output": "Widget A",
                    "data_point_in_document": "Item descriptions in line items table"
                },
                {
                    "field_name": "line_items[*].quantity",
                    "instruction": "Extract quantity",
                    "expected_output": "2",
                    "data_point_in_document": "Quantities in line items table"
                },
                {
                    "field_name": "line_items[*].unit_price",
                    "instruction": "Extract unit price",
                    "expected_output": "25.00",
                    "data_point_in_document": "Unit prices in line items table"
                },
                {
                    "field_name": "total",
                    "instruction": "Extract total amount",
                    "expected_output": "108.50",
                    "data_point_in_document": "Total at bottom of invoice"
                }
            ]
        }

    def test_schema_flattening_and_unflattening(self, sample_nested_blueprint):
        """Test that schema flattening and unflattening work correctly."""
        # Create temporary schema file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_nested_blueprint, f)
            schema_path = f.name

        try:
            # Load original schema
            original_schema = Schema.from_file(schema_path)
            
            # Verify it's detected as nested
            assert original_schema.is_nested() is True
            
            # Flatten the schema
            flattened_schema, path_mapping = original_schema.flatten_for_optimization()
            
            # Verify flattened field names
            expected_flat_fields = [
                "invoice_number",
                "date", 
                "customer.name",
                "customer.email",
                "customer.address.street",
                "customer.address.city",
                "customer.address.state",
                "customer.address.zip_code",
                "line_items[*].description",
                "line_items[*].quantity",
                "line_items[*].unit_price",
                "line_items[*].total_price",
                "subtotal",
                "tax",
                "total"
            ]
            
            for field in expected_flat_fields:
                assert field in flattened_schema.properties, f"Field {field} not found in flattened schema"
            
            # Verify path mapping was created
            assert len(path_mapping) > 0
            
            # Modify some instructions in flattened schema
            flattened_schema.properties["customer.name"].instruction = "OPTIMIZED: Extract the full customer name"
            flattened_schema.properties["line_items[*].unit_price"].instruction = "OPTIMIZED: Extract unit price with currency"
            
            # Unflatten back to nested structure
            reconstructed_schema = original_schema.unflatten_from_optimization(
                flattened_schema, path_mapping
            )
            
            # Verify nested structure is reconstructed
            assert "customer" in reconstructed_schema.properties
            assert "line_items" in reconstructed_schema.properties
            
            # Verify optimized instructions are preserved in nested structure
            customer_props = reconstructed_schema.properties["customer"]["properties"]
            assert customer_props["name"]["instruction"] == "OPTIMIZED: Extract the full customer name"
            
            line_items_props = reconstructed_schema.properties["line_items"]["items"]["properties"]
            assert line_items_props["unit_price"]["instruction"] == "OPTIMIZED: Extract unit price with currency"
            
            # Verify other nested fields remain unchanged
            assert customer_props["email"]["instruction"] == "Extract customer email address"
            
        finally:
            os.unlink(schema_path)

    @patch('src.models.optimizer.BDAClient')
    @patch('src.models.optimizer.BDAConfig')
    def test_complete_nested_blueprint_workflow(self, mock_config_class, 
                                              mock_bda_client_class, sample_nested_blueprint, 
                                              sample_config_data):
        """Test complete workflow: import → flatten → optimize → unflatten → export."""
        
        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_file:
            json.dump(sample_nested_blueprint, schema_file)
            schema_path = schema_file.name
            
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(sample_config_data, config_file)
            config_path = config_file.name

        try:
            # Mock configuration
            mock_config = Mock()
            mock_config.inputs = []
            for input_data in sample_config_data["inputs"]:
                mock_input = Mock()
                mock_input.field_name = input_data["field_name"]
                mock_input.instruction = input_data["instruction"]
                mock_input.expected_output = input_data["expected_output"]
                mock_input.data_point_in_document = input_data["data_point_in_document"]
                mock_config.inputs.append(mock_input)
            
            mock_config.input_document = sample_config_data["document_s3_uri"]
            mock_config_class.from_file.return_value = mock_config

            # Mock BDA client
            mock_bda_client = Mock()
            mock_bda_client_class.from_config.return_value = mock_bda_client

            # Mock the get_blueprint_schema_to_file method with proper directory creation
            def mock_get_schema(path):
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'w') as f:
                    json.dump(sample_nested_blueprint, f)

            mock_bda_client.get_blueprint_schema_to_file.side_effect = mock_get_schema
            
            # Mock BDA operations
            mock_bda_client.create_test_blueprint.return_value = {
                "blueprint": {"blueprintArn": "test-arn"}
            }
            mock_bda_client.update_test_blueprint.return_value = True
            mock_bda_client.delete_test_blueprint.return_value = True
            mock_bda_client.update_customer_blueprint.return_value = True
            
            # Mock BDA job results with improved similarities
            mock_df = Mock()
            mock_similarities = {
                "invoice_number": 0.95,
                "customer.name": 0.85,
                "customer.email": 0.90,
                "customer.address.street": 0.88,
                "customer.address.city": 0.92,
                "line_items[*].description": 0.87,
                "line_items[*].quantity": 0.93,
                "line_items[*].unit_price": 0.89,
                "total": 0.91
            }
            mock_bda_client.run_bda_job.return_value = (mock_df, mock_similarities, True)

            # Create optimizer
            optimizer = SequentialOptimizer.from_config_file(
                config_file=config_path,
                threshold=0.8,
                use_doc=False,
                use_template=True,
                model_choice="test-model",
                max_iterations=2
            )

            # STEP 1: Verify import and nested detection
            assert optimizer.is_nested_blueprint is True, "Should detect nested blueprint"
            assert len(optimizer.path_mapping) > 0, "Should create path mapping"
            assert optimizer.original_schema is not None, "Should store original schema"

            # STEP 2: Verify flattening occurred
            expected_flat_fields = [
                "customer.name", "customer.email", "customer.address.street", 
                "customer.address.city", "line_items[*].description", 
                "line_items[*].quantity", "line_items[*].unit_price"
            ]
            
            for field in expected_flat_fields:
                assert field in optimizer.schema.properties, f"Flattened field {field} should exist"

            # STEP 3: Run optimization workflow
            with patch('src.models.optimizer.extract_inputs_to_dataframe_from_file') as mock_extract:
                mock_extract.return_value = Mock()
                
                # Mock the run method's file operations
                with patch('builtins.open', create=True) as mock_open:
                    mock_file = Mock()
                    mock_open.return_value.__enter__.return_value = mock_file
                    
                    # Run optimization
                    final_report_path = optimizer.run(max_iterations=2)

            # STEP 4: Verify optimization completed
            assert final_report_path is not None, "Should return final report path"
            
            # STEP 5: Verify unflattening and export
            # Check that update_schema_with_instructions was called and handled nested structure
            assert mock_bda_client.update_test_blueprint.called, "Should update test blueprint"
            assert mock_bda_client.update_customer_blueprint.called, "Should update customer blueprint"
            
            # Verify that the optimizer maintains both flattened and nested schemas
            assert optimizer.schema is not None, "Should maintain flattened schema"
            assert optimizer.original_schema is not None, "Should maintain original nested schema"
            assert optimizer.path_mapping, "Should maintain path mapping"

        finally:
            os.unlink(schema_path)
            os.unlink(config_path)

    def test_nested_blueprint_bda_compatibility(self, sample_nested_blueprint):
        """Test that exported nested blueprint is compatible with BDA service format."""
        
        # Create temporary schema file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_nested_blueprint, f)
            schema_path = f.name

        try:
            # Load and process schema
            original_schema = Schema.from_file(schema_path)
            flattened_schema, path_mapping = original_schema.flatten_for_optimization()
            
            # Simulate optimization by modifying instructions
            flattened_schema.properties["customer.name"].instruction = "Extract the complete customer name from the document"
            flattened_schema.properties["line_items[*].unit_price"].instruction = "Extract the unit price including currency symbol"
            
            # Reconstruct nested schema
            reconstructed_schema = original_schema.unflatten_from_optimization(
                flattened_schema, path_mapping
            )
            
            # Save reconstructed schema
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as output_file:
                reconstructed_schema.to_file(output_file.name)
                
                # Load and verify the exported schema
                with open(output_file.name, 'r') as f:
                    exported_schema = json.load(f)
                
                # Verify BDA compatibility requirements
                
                # 1. Must have required top-level fields
                assert "$schema" in exported_schema, "Must have $schema field"
                assert "type" in exported_schema, "Must have type field"
                assert "properties" in exported_schema, "Must have properties field"
                assert exported_schema["type"] == "object", "Root type must be object"
                
                # 2. Nested structure must be preserved
                assert "customer" in exported_schema["properties"], "Customer object must exist"
                assert "line_items" in exported_schema["properties"], "Line items array must exist"
                
                # 3. Object properties must have correct structure
                customer_obj = exported_schema["properties"]["customer"]
                assert customer_obj["type"] == "object", "Customer must be object type"
                assert "properties" in customer_obj, "Customer must have properties"
                
                # 4. Array items must have correct structure
                line_items_array = exported_schema["properties"]["line_items"]
                assert line_items_array["type"] == "array", "Line items must be array type"
                assert "items" in line_items_array, "Array must have items definition"
                assert line_items_array["items"]["type"] == "object", "Array items must be objects"
                
                # 5. Optimized instructions must be preserved
                customer_name = customer_obj["properties"]["name"]
                assert customer_name["instruction"] == "Extract the complete customer name from the document"
                
                line_item_price = line_items_array["items"]["properties"]["unit_price"]
                assert line_item_price["instruction"] == "Extract the unit price including currency symbol"
                
                # 6. Original field properties must be preserved
                assert customer_name["type"] == "string", "Field types must be preserved"
                assert customer_name["inferenceType"] == "explicit", "Inference types must be preserved"
                
                # 7. Nested address structure must be intact
                address_obj = customer_obj["properties"]["address"]
                assert address_obj["type"] == "object", "Address must be object"
                assert "street" in address_obj["properties"], "Address must have street"
                assert "city" in address_obj["properties"], "Address must have city"
                
                os.unlink(output_file.name)

        finally:
            os.unlink(schema_path)

    def test_backward_compatibility_with_flat_blueprints(self, sample_config_data):
        """Test that the system maintains backward compatibility with flat blueprints."""
        
        # Create a flat blueprint (no nested structures)
        flat_blueprint = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Simple flat invoice schema",
            "class": "invoice",
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "inferenceType": "explicit",
                    "instruction": "Extract invoice number"
                },
                "customer_name": {
                    "type": "string",
                    "inferenceType": "explicit",
                    "instruction": "Extract customer name"
                },
                "total_amount": {
                    "type": "number",
                    "inferenceType": "explicit",
                    "instruction": "Extract total amount"
                }
            }
        }
        
        # Update config for flat fields
        flat_config_data = sample_config_data.copy()
        flat_config_data["inputs"] = [
            {
                "field_name": "invoice_number",
                "instruction": "Extract invoice number",
                "expected_output": "INV-12345",
                "data_point_in_document": "Invoice number at top"
            },
            {
                "field_name": "customer_name",
                "instruction": "Extract customer name",
                "expected_output": "John Smith",
                "data_point_in_document": "Customer name in header"
            },
            {
                "field_name": "total_amount",
                "instruction": "Extract total amount",
                "expected_output": "108.50",
                "data_point_in_document": "Total at bottom"
            }
        ]

        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_file:
            json.dump(flat_blueprint, schema_file)
            schema_path = schema_file.name
            
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
            json.dump(flat_config_data, config_file)
            config_path = config_file.name

        try:
            with patch('src.models.optimizer.BDAClient') as mock_bda_client_class:
                with patch('src.models.optimizer.BDAConfig') as mock_config_class:
                    # Mock configuration
                    mock_config = Mock()
                    mock_config.inputs = []
                    for input_data in flat_config_data["inputs"]:
                        mock_input = Mock()
                        mock_input.field_name = input_data["field_name"]
                        mock_input.instruction = input_data["instruction"]
                        mock_input.expected_output = input_data["expected_output"]
                        mock_input.data_point_in_document = input_data["data_point_in_document"]
                        mock_config.inputs.append(mock_input)
                    
                    mock_config.input_document = flat_config_data["document_s3_uri"]
                    mock_config_class.from_file.return_value = mock_config

                    # Mock BDA client
                    mock_bda_client = Mock()
                    mock_bda_client_class.from_config.return_value = mock_bda_client

                    # Mock the get_blueprint_schema_to_file method with proper directory creation
                    def mock_get_schema(path):
                        # Create directory if it doesn't exist
                        os.makedirs(os.path.dirname(path), exist_ok=True)
                        with open(path, 'w') as f:
                            json.dump(flat_blueprint, f)

                    mock_bda_client.get_blueprint_schema_to_file.side_effect = mock_get_schema

                    # Create optimizer
                    optimizer = SequentialOptimizer.from_config_file(
                        config_file=config_path,
                        threshold=0.8,
                        use_doc=False,
                        use_template=True,
                        model_choice="test-model",
                        max_iterations=2
                    )

                    # Verify flat blueprint handling
                    assert optimizer.is_nested_blueprint is False, "Should detect flat blueprint"
                    assert len(optimizer.path_mapping) == 0, "Should have empty path mapping"
                    assert optimizer.original_schema is not None, "Should store original schema"

                    # Verify schema remains unchanged for flat blueprints
                    expected_flat_fields = ["invoice_number", "customer_name", "total_amount"]
                    for field in expected_flat_fields:
                        assert field in optimizer.schema.properties, f"Field {field} should exist unchanged"

                    # Verify no flattening occurred
                    assert "customer.name" not in optimizer.schema.properties, "Should not have dot-notation fields"
                    assert "line_items[*].price" not in optimizer.schema.properties, "Should not have array notation fields"

        finally:
            os.unlink(schema_path)
            os.unlink(config_path)

    def test_error_handling_malformed_nested_schema(self):
        """Test error handling for malformed nested schemas."""
        
        # Create malformed nested schema (missing properties in object)
        malformed_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Malformed schema for testing",
            "class": "test",
            "type": "object",
            "properties": {
                "customer": {
                    "type": "object"
                    # Missing "properties" field
                },
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object"
                        # Missing "properties" field
                    }
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(malformed_schema, f)
            schema_path = f.name

        try:
            schema = Schema.from_file(schema_path)
            
            # Should not be detected as nested due to missing properties
            assert schema.is_nested() is False, "Malformed schema should not be detected as nested"
            
            # Flattening should return original schema unchanged
            flattened_schema, path_mapping = schema.flatten_for_optimization()
            assert len(path_mapping) == 0, "Should have empty path mapping for malformed schema"
            assert flattened_schema.properties == schema.properties, "Should return unchanged schema"

        finally:
            os.unlink(schema_path)

    def test_complex_nested_structure_handling(self):
        """Test handling of complex nested structures with multiple levels."""
        
        complex_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Complex nested schema for testing",
            "class": "order",
            "type": "object",
            "properties": {
                "order": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string",
                            "inferenceType": "explicit",
                            "instruction": "Extract order ID"
                        },
                        "customer": {
                            "type": "object",
                            "properties": {
                                "personal": {
                                    "type": "object",
                                    "properties": {
                                        "first_name": {
                                            "type": "string",
                                            "inferenceType": "explicit",
                                            "instruction": "Extract first name"
                                        },
                                        "last_name": {
                                            "type": "string",
                                            "inferenceType": "explicit",
                                            "instruction": "Extract last name"
                                        }
                                    }
                                },
                                "contacts": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "type": {
                                                "type": "string",
                                                "inferenceType": "explicit",
                                                "instruction": "Extract contact type"
                                            },
                                            "value": {
                                                "type": "string",
                                                "inferenceType": "explicit",
                                                "instruction": "Extract contact value"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(complex_schema, f)
            schema_path = f.name

        try:
            schema = Schema.from_file(schema_path)
            
            # Should be detected as nested
            assert schema.is_nested() is True, "Complex schema should be detected as nested"
            
            # Flatten the schema
            flattened_schema, path_mapping = schema.flatten_for_optimization()
            
            # Verify complex flattened field names
            expected_complex_fields = [
                "order.id",
                "order.customer.personal.first_name",
                "order.customer.personal.last_name",
                "order.customer.contacts[*].type",
                "order.customer.contacts[*].value"
            ]
            
            for field in expected_complex_fields:
                assert field in flattened_schema.properties, f"Complex field {field} should be flattened"
            
            # Modify instructions
            flattened_schema.properties["order.customer.personal.first_name"].instruction = "OPTIMIZED: Extract customer's first name"
            flattened_schema.properties["order.customer.contacts[*].value"].instruction = "OPTIMIZED: Extract contact information"
            
            # Unflatten back to nested structure
            reconstructed_schema = schema.unflatten_from_optimization(flattened_schema, path_mapping)
            
            # Verify complex nested structure is reconstructed
            order_props = reconstructed_schema.properties["order"]["properties"]
            customer_props = order_props["customer"]["properties"]
            personal_props = customer_props["personal"]["properties"]
            contacts_props = customer_props["contacts"]["items"]["properties"]
            
            # Verify optimized instructions are preserved in deeply nested structure
            assert personal_props["first_name"]["instruction"] == "OPTIMIZED: Extract customer's first name"
            assert contacts_props["value"]["instruction"] == "OPTIMIZED: Extract contact information"
            
            # Verify unchanged instructions remain
            assert personal_props["last_name"]["instruction"] == "Extract last name"
            assert contacts_props["type"]["instruction"] == "Extract contact type"

        finally:
            os.unlink(schema_path)