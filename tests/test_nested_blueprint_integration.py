"""
Integration tests for nested blueprint processing workflow.
Tests the complete workflow: import → flatten → optimize → unflatten → export
"""
import pytest
import json
import os
import tempfile
import shutil
from pathlib import Path

from src.models.schema import Schema
from src.services.schema_converter import SchemaFlattener, SchemaUnflattener


class TestNestedBlueprintIntegration:
    """Integration tests for nested blueprint processing."""

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

    def test_end_to_end_nested_blueprint_processing(self, sample_nested_blueprint):
        """Test complete workflow: import → flatten → optimize → unflatten → export."""
        
        # Create temporary schema file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_nested_blueprint, f)
            schema_path = f.name

        try:
            # STEP 1: Import - Load original nested schema
            original_schema = Schema.from_file(schema_path)
            
            # Verify it's detected as nested
            assert original_schema.is_nested() is True, "Should detect nested blueprint"
            
            # STEP 2: Flatten - Convert to dot-notation for optimization
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
            assert len(path_mapping) > 0, "Should create path mapping"
            
            # STEP 3: Optimize - Simulate optimization by modifying instructions
            flattened_schema.properties["customer.name"].instruction = "OPTIMIZED: Extract the complete customer name from the billing section"
            flattened_schema.properties["customer.email"].instruction = "OPTIMIZED: Extract customer email address with validation"
            flattened_schema.properties["line_items[*].unit_price"].instruction = "OPTIMIZED: Extract unit price including currency symbol"
            flattened_schema.properties["line_items[*].description"].instruction = "OPTIMIZED: Extract detailed item description with specifications"
            
            # STEP 4: Unflatten - Reconstruct nested structure
            reconstructed_schema = original_schema.unflatten_from_optimization(
                flattened_schema, path_mapping
            )
            
            # STEP 5: Export - Verify nested structure is reconstructed correctly
            assert "customer" in reconstructed_schema.properties, "Customer object must exist"
            assert "line_items" in reconstructed_schema.properties, "Line items array must exist"
            
            # Verify optimized instructions are preserved in nested structure
            customer_props = reconstructed_schema.properties["customer"]["properties"]
            assert customer_props["name"]["instruction"] == "OPTIMIZED: Extract the complete customer name from the billing section"
            assert customer_props["email"]["instruction"] == "OPTIMIZED: Extract customer email address with validation"
            
            line_items_props = reconstructed_schema.properties["line_items"]["items"]["properties"]
            assert line_items_props["unit_price"]["instruction"] == "OPTIMIZED: Extract unit price including currency symbol"
            assert line_items_props["description"]["instruction"] == "OPTIMIZED: Extract detailed item description with specifications"
            
            # Verify unchanged instructions remain
            assert customer_props["address"]["properties"]["street"]["instruction"] == "Extract street address"
            assert line_items_props["quantity"]["instruction"] == "Extract quantity"
            
            # STEP 6: Verify BDA compatibility - Save and reload
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as output_file:
                reconstructed_schema.to_file(output_file.name)
                
                # Load and verify the exported schema
                with open(output_file.name, 'r') as f:
                    exported_schema = json.load(f)
                
                # Verify BDA compatibility requirements
                assert "$schema" in exported_schema, "Must have $schema field"
                assert "type" in exported_schema, "Must have type field"
                assert "properties" in exported_schema, "Must have properties field"
                assert exported_schema["type"] == "object", "Root type must be object"
                
                # Verify nested structure is preserved
                assert "customer" in exported_schema["properties"], "Customer object must exist"
                assert "line_items" in exported_schema["properties"], "Line items array must exist"
                
                # Verify object properties have correct structure
                customer_obj = exported_schema["properties"]["customer"]
                assert customer_obj["type"] == "object", "Customer must be object type"
                assert "properties" in customer_obj, "Customer must have properties"
                
                # Verify array items have correct structure
                line_items_array = exported_schema["properties"]["line_items"]
                assert line_items_array["type"] == "array", "Line items must be array type"
                assert "items" in line_items_array, "Array must have items definition"
                assert line_items_array["items"]["type"] == "object", "Array items must be objects"
                
                # Verify optimized instructions are preserved
                customer_name = customer_obj["properties"]["name"]
                assert customer_name["instruction"] == "OPTIMIZED: Extract the complete customer name from the billing section"
                
                line_item_price = line_items_array["items"]["properties"]["unit_price"]
                assert line_item_price["instruction"] == "OPTIMIZED: Extract unit price including currency symbol"
                
                # Verify original field properties are preserved
                assert customer_name["type"] == "string", "Field types must be preserved"
                assert customer_name["inferenceType"] == "explicit", "Inference types must be preserved"
                
                os.unlink(output_file.name)

        finally:
            os.unlink(schema_path)

    def test_backward_compatibility_with_flat_blueprints(self):
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

        # Create temporary schema file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(flat_blueprint, f)
            schema_path = f.name

        try:
            # Load schema
            schema = Schema.from_file(schema_path)
            
            # Verify it's detected as flat
            assert schema.is_nested() is False, "Should detect flat blueprint"
            
            # Flatten should return unchanged schema
            flattened_schema, path_mapping = schema.flatten_for_optimization()
            
            # Verify no path mapping for flat schemas
            assert len(path_mapping) == 0, "Should have empty path mapping"
            
            # Verify schema remains unchanged for flat blueprints
            expected_flat_fields = ["invoice_number", "customer_name", "total_amount"]
            for field in expected_flat_fields:
                assert field in flattened_schema.properties, f"Field {field} should exist unchanged"
            
            # Verify no dot-notation fields were created
            assert "customer.name" not in flattened_schema.properties, "Should not have dot-notation fields"
            assert "line_items[*].price" not in flattened_schema.properties, "Should not have array notation fields"
            
            # Simulate optimization
            flattened_schema.properties["customer_name"].instruction = "OPTIMIZED: Extract full customer name"
            
            # Unflatten should return the same schema since it was already flat
            reconstructed_schema = schema.unflatten_from_optimization(flattened_schema, path_mapping)
            
            # Verify optimization was preserved
            assert reconstructed_schema.properties["customer_name"].instruction == "OPTIMIZED: Extract full customer name"

        finally:
            os.unlink(schema_path)

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
            
            # Should handle gracefully without errors
            assert flattened_schema is not None, "Should return a valid schema"

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

    def test_real_world_nested_blueprint_from_samples(self):
        """Test with the actual nested blueprint sample from the project."""
        
        # Use the actual sample file from the project
        sample_path = "samples/nested_invoice_blueprint.json"
        
        if not os.path.exists(sample_path):
            pytest.skip("Sample nested blueprint file not found")
        
        # Load the real sample
        schema = Schema.from_file(sample_path)
        
        # Verify it's detected as nested
        assert schema.is_nested() is True, "Sample should be detected as nested"
        
        # Test the complete workflow
        flattened_schema, path_mapping = schema.flatten_for_optimization()
        
        # Verify some expected fields from the sample
        expected_fields = [
            "customer.name",
            "customer.address.street", 
            "line_items[*].description",
            "line_items[*].unit_price"
        ]
        
        for field in expected_fields:
            assert field in flattened_schema.properties, f"Expected field {field} should be flattened"
        
        # Simulate optimization
        original_instruction = flattened_schema.properties["customer.name"].instruction
        flattened_schema.properties["customer.name"].instruction = f"OPTIMIZED: {original_instruction}"
        
        # Reconstruct
        reconstructed_schema = schema.unflatten_from_optimization(flattened_schema, path_mapping)
        
        # Verify optimization was preserved
        customer_name_instruction = reconstructed_schema.properties["customer"]["properties"]["name"]["instruction"]
        assert customer_name_instruction.startswith("OPTIMIZED:"), "Optimization should be preserved"
        
        # Save and verify BDA compatibility
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            reconstructed_schema.to_file(f.name)
            
            # Verify the file can be loaded as valid JSON
            with open(f.name, 'r') as verify_file:
                exported_data = json.load(verify_file)
                assert exported_data["type"] == "object", "Should be valid BDA schema"
                assert "customer" in exported_data["properties"], "Should preserve nested structure"
            
            os.unlink(f.name)