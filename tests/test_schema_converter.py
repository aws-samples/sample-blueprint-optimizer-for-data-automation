"""
Unit tests for schema conversion utilities.

Tests the SchemaFlattener and SchemaUnflattener classes that handle
conversion between nested and flat blueprint structures.
"""
import pytest
from src.services.schema_converter import SchemaFlattener, SchemaUnflattener


class TestSchemaFlattener:
    """Test cases for SchemaFlattener class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.flattener = SchemaFlattener()
    
    def test_is_nested_schema_with_flat_structure(self):
        """Test detection of flat schema structure."""
        flat_schema = {
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                },
                "total_amount": {
                    "type": "number",
                    "instruction": "Extract total amount"
                }
            }
        }
        
        assert not self.flattener.is_nested_schema(flat_schema)
    
    def test_is_nested_schema_with_object_nesting(self):
        """Test detection of nested schema with object properties."""
        nested_schema = {
            "properties": {
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract customer name"
                        }
                    }
                }
            }
        }
        
        assert self.flattener.is_nested_schema(nested_schema)
    
    def test_is_nested_schema_with_array_of_objects(self):
        """Test detection of nested schema with array of objects."""
        nested_schema = {
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "price": {
                                "type": "number",
                                "instruction": "Extract item price"
                            }
                        }
                    }
                }
            }
        }
        
        assert self.flattener.is_nested_schema(nested_schema)
    
    def test_is_nested_schema_with_simple_array(self):
        """Test detection with simple array (not nested)."""
        schema_with_simple_array = {
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            }
        }
        
        assert not self.flattener.is_nested_schema(schema_with_simple_array)
    
    def test_flatten_schema_with_flat_structure(self):
        """Test flattening of already flat schema."""
        flat_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                },
                "total_amount": {
                    "type": "number",
                    "instruction": "Extract total amount"
                }
            }
        }
        
        flattened, path_mapping = self.flattener.flatten_schema(flat_schema)
        
        # Should return unchanged
        assert flattened == flat_schema
        assert path_mapping == {}
    
    def test_flatten_schema_with_simple_object_nesting(self):
        """Test flattening of schema with simple object nesting."""
        nested_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract customer name"
                        },
                        "email": {
                            "type": "string",
                            "instruction": "Extract customer email"
                        }
                    }
                },
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                }
            }
        }
        
        flattened, path_mapping = self.flattener.flatten_schema(nested_schema)
        
        # Check flattened properties
        expected_properties = {
            "customer.name": {
                "type": "string",
                "instruction": "Extract customer name"
            },
            "customer.email": {
                "type": "string",
                "instruction": "Extract customer email"
            },
            "invoice_number": {
                "type": "string",
                "instruction": "Extract invoice number"
            }
        }
        
        assert flattened["properties"] == expected_properties
        assert "customer.name" in path_mapping
        assert "customer.email" in path_mapping
        assert "invoice_number" in path_mapping
    
    def test_flatten_schema_with_array_of_objects(self):
        """Test flattening of schema with array of objects."""
        nested_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "instruction": "Extract item name"
                            },
                            "price": {
                                "type": "number",
                                "instruction": "Extract item price"
                            }
                        }
                    }
                }
            }
        }
        
        flattened, path_mapping = self.flattener.flatten_schema(nested_schema)
        
        # Check flattened properties
        expected_properties = {
            "items[*].name": {
                "type": "string",
                "instruction": "Extract item name"
            },
            "items[*].price": {
                "type": "number",
                "instruction": "Extract item price"
            }
        }
        
        assert flattened["properties"] == expected_properties
        assert "items[*].name" in path_mapping
        assert "items[*].price" in path_mapping
    
    def test_flatten_schema_with_deep_nesting(self):
        """Test flattening of schema with multiple levels of nesting."""
        nested_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract customer name"
                        },
                        "address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                    "type": "string",
                                    "instruction": "Extract street address"
                                },
                                "city": {
                                    "type": "string",
                                    "instruction": "Extract city"
                                }
                            }
                        }
                    }
                }
            }
        }
        
        flattened, path_mapping = self.flattener.flatten_schema(nested_schema)
        
        # Check flattened properties
        expected_properties = {
            "customer.name": {
                "type": "string",
                "instruction": "Extract customer name"
            },
            "customer.address.street": {
                "type": "string",
                "instruction": "Extract street address"
            },
            "customer.address.city": {
                "type": "string",
                "instruction": "Extract city"
            }
        }
        
        assert flattened["properties"] == expected_properties
        assert len(path_mapping) == 3
    
    def test_flatten_schema_with_mixed_structures(self):
        """Test flattening of schema with mixed flat and nested structures."""
        mixed_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                },
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract customer name"
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
                                "instruction": "Extract item price"
                            }
                        }
                    }
                },
                "total": {
                    "type": "number",
                    "instruction": "Extract total amount"
                }
            }
        }
        
        flattened, path_mapping = self.flattener.flatten_schema(mixed_schema)
        
        # Check that all field types are handled correctly
        expected_fields = {
            "invoice_number",
            "customer.name", 
            "items[*].price",
            "total"
        }
        
        assert set(flattened["properties"].keys()) == expected_fields
        assert len(path_mapping) == 4
    
    def test_flatten_schema_preserves_metadata(self):
        """Test that flattening preserves schema metadata."""
        nested_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Test schema",
            "class": "invoice",
            "type": "object",
            "definitions": {"test": "value"},
            "properties": {
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract customer name"
                        }
                    }
                }
            }
        }
        
        flattened, path_mapping = self.flattener.flatten_schema(nested_schema)
        
        # Check that metadata is preserved
        assert flattened["$schema"] == nested_schema["$schema"]
        assert flattened["description"] == nested_schema["description"]
        assert flattened["class"] == nested_schema["class"]
        assert flattened["type"] == nested_schema["type"]
        assert flattened["definitions"] == nested_schema["definitions"]


class TestSchemaUnflattener:
    """Test cases for SchemaUnflattener class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.unflattener = SchemaUnflattener()
    
    def test_unflatten_schema_with_no_mapping(self):
        """Test unflattening when no path mapping exists (flat schema)."""
        flat_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                }
            }
        }
        
        result = self.unflattener.unflatten_schema(flat_schema, {})
        
        # Should return unchanged
        assert result == flat_schema
    
    def test_unflatten_simple_object_nesting(self):
        """Test unflattening of simple object nesting."""
        flat_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "customer.name": {
                    "type": "string",
                    "instruction": "Extract customer name"
                },
                "customer.email": {
                    "type": "string",
                    "instruction": "Extract customer email"
                },
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                }
            }
        }
        
        path_mapping = {
            "customer.name": "customer.properties.name",
            "customer.email": "customer.properties.email",
            "invoice_number": "invoice_number"
        }
        
        result = self.unflattener.unflatten_schema(flat_schema, path_mapping)
        
        # Check reconstructed structure
        expected_properties = {
            "customer": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "instruction": "Extract customer name"
                    },
                    "email": {
                        "type": "string",
                        "instruction": "Extract customer email"
                    }
                }
            },
            "invoice_number": {
                "type": "string",
                "instruction": "Extract invoice number"
            }
        }
        
        assert result["properties"] == expected_properties
    
    def test_unflatten_array_of_objects(self):
        """Test unflattening of array of objects."""
        flat_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "items[*].name": {
                    "type": "string",
                    "instruction": "Extract item name"
                },
                "items[*].price": {
                    "type": "number",
                    "instruction": "Extract item price"
                }
            }
        }
        
        path_mapping = {
            "items[*].name": "items.items.properties.name",
            "items[*].price": "items.items.properties.price"
        }
        
        result = self.unflattener.unflatten_schema(flat_schema, path_mapping)
        
        # Check reconstructed structure
        expected_properties = {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract item name"
                        },
                        "price": {
                            "type": "number",
                            "instruction": "Extract item price"
                        }
                    }
                }
            }
        }
        
        assert result["properties"] == expected_properties
    
    def test_unflatten_deep_nesting(self):
        """Test unflattening of deeply nested structures."""
        flat_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "customer.address.street": {
                    "type": "string",
                    "instruction": "Extract street address"
                },
                "customer.address.city": {
                    "type": "string",
                    "instruction": "Extract city"
                },
                "customer.name": {
                    "type": "string",
                    "instruction": "Extract customer name"
                }
            }
        }
        
        path_mapping = {
            "customer.address.street": "customer.properties.address.properties.street",
            "customer.address.city": "customer.properties.address.properties.city",
            "customer.name": "customer.properties.name"
        }
        
        result = self.unflattener.unflatten_schema(flat_schema, path_mapping)
        
        # Check reconstructed structure
        expected_properties = {
            "customer": {
                "type": "object",
                "properties": {
                    "address": {
                        "type": "object",
                        "properties": {
                            "street": {
                                "type": "string",
                                "instruction": "Extract street address"
                            },
                            "city": {
                                "type": "string",
                                "instruction": "Extract city"
                            }
                        }
                    },
                    "name": {
                        "type": "string",
                        "instruction": "Extract customer name"
                    }
                }
            }
        }
        
        assert result["properties"] == expected_properties
    
    def test_unflatten_preserves_metadata(self):
        """Test that unflattening preserves schema metadata."""
        flat_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Test schema",
            "class": "invoice",
            "type": "object",
            "definitions": {"test": "value"},
            "properties": {
                "customer.name": {
                    "type": "string",
                    "instruction": "Extract customer name"
                }
            }
        }
        
        path_mapping = {
            "customer.name": "customer.properties.name"
        }
        
        result = self.unflattener.unflatten_schema(flat_schema, path_mapping)
        
        # Check that metadata is preserved
        assert result["$schema"] == flat_schema["$schema"]
        assert result["description"] == flat_schema["description"]
        assert result["class"] == flat_schema["class"]
        assert result["type"] == flat_schema["type"]
        assert result["definitions"] == flat_schema["definitions"]


class TestRoundTripConversion:
    """Test round-trip conversion (flatten -> unflatten)."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.flattener = SchemaFlattener()
        self.unflattener = SchemaUnflattener()
    
    def test_round_trip_simple_nesting(self):
        """Test that flatten -> unflatten preserves original structure."""
        original_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Test schema",
            "type": "object",
            "properties": {
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract customer name"
                        },
                        "email": {
                            "type": "string",
                            "instruction": "Extract customer email"
                        }
                    }
                },
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                }
            }
        }
        
        # Flatten then unflatten
        flattened, path_mapping = self.flattener.flatten_schema(original_schema)
        reconstructed = self.unflattener.unflatten_schema(flattened, path_mapping)
        
        # Should match original
        assert reconstructed == original_schema
    
    def test_round_trip_array_nesting(self):
        """Test round-trip conversion with array nesting."""
        original_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "instruction": "Extract item name"
                            },
                            "price": {
                                "type": "number",
                                "instruction": "Extract item price"
                            }
                        }
                    }
                }
            }
        }
        
        # Flatten then unflatten
        flattened, path_mapping = self.flattener.flatten_schema(original_schema)
        reconstructed = self.unflattener.unflatten_schema(flattened, path_mapping)
        
        # Should match original
        assert reconstructed == original_schema
    
    def test_round_trip_complex_nesting(self):
        """Test round-trip conversion with complex mixed nesting."""
        original_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "Complex test schema",
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "instruction": "Extract invoice number"
                },
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "instruction": "Extract customer name"
                        },
                        "address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                    "type": "string",
                                    "instruction": "Extract street"
                                },
                                "city": {
                                    "type": "string",
                                    "instruction": "Extract city"
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
                            "name": {
                                "type": "string",
                                "instruction": "Extract item name"
                            },
                            "price": {
                                "type": "number",
                                "instruction": "Extract item price"
                            }
                        }
                    }
                },
                "total": {
                    "type": "number",
                    "instruction": "Extract total amount"
                }
            }
        }
        
        # Flatten then unflatten
        flattened, path_mapping = self.flattener.flatten_schema(original_schema)
        reconstructed = self.unflattener.unflatten_schema(flattened, path_mapping)
        
        # Should match original
        assert reconstructed == original_schema