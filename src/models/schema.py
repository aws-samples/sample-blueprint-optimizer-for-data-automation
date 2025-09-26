"""
Schema models for the BDA optimization application.
"""
from typing import Dict, Any, Optional, List, Tuple
from pydantic import BaseModel, Field
import logging

logger = logging.getLogger(__name__)


class SchemaProperty(BaseModel):
    """
    Represents a property in the JSON schema.
    """
    type: str = Field(description="The data type of the property")
    inferenceType: str = Field(description="The inference type (e.g., 'explicit')")
    instruction: str = Field(description="The instruction for extracting this property")


class Schema(BaseModel):
    """
    Represents the JSON schema for the blueprint.
    """
    schema: str = Field(default="http://json-schema.org/draft-07/schema#", alias="$schema", description="The JSON schema version")
    description: str = Field(description="Description of the document")
    class_: str = Field(alias="class", description="The document class")
    type: str = Field(default="object", description="The schema type")
    definitions: Dict[str, Any] = Field(default_factory=dict, description="Schema definitions")
    properties: Dict[str, Any] = Field(description="Schema properties (can be SchemaProperty instances or nested structures)")

    @classmethod
    def from_file(cls, file_path: str) -> "Schema":
        """
        Load schema from a JSON file.
        Handles both flat and nested blueprint structures.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Schema: Loaded schema (preserves original structure)
        """
        import json
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Convert properties to SchemaProperty instances if they exist
        if "properties" in data:
            properties = {}
            for field_name, prop_def in data["properties"].items():
                if isinstance(prop_def, dict) and all(key in prop_def for key in ["type", "inferenceType", "instruction"]):
                    # This is a flat property that can be converted to SchemaProperty
                    properties[field_name] = SchemaProperty(**prop_def)
                else:
                    # This might be a nested property, keep as-is for now
                    # The Schema model will handle it appropriately
                    properties[field_name] = prop_def
            data["properties"] = properties
        
        return cls(**data)
    
    def to_file(self, file_path: str) -> None:
        """
        Save schema to a JSON file.
        Preserves nested structure if present.
        
        Args:
            file_path: Path to save the JSON file
        """
        import json
        
        # Get the schema as a dictionary
        schema_dict = self.model_dump(by_alias=True)
        
        # Convert SchemaProperty instances back to dictionaries for JSON serialization
        if "properties" in schema_dict:
            properties = {}
            for field_name, prop_def in schema_dict["properties"].items():
                if hasattr(prop_def, 'model_dump'):
                    # This is a SchemaProperty instance
                    properties[field_name] = prop_def.model_dump()
                else:
                    # This is already a dictionary (nested structure)
                    properties[field_name] = prop_def
            schema_dict["properties"] = properties
        
        with open(file_path, 'w') as f:
            json.dump(schema_dict, f, indent=4)
    
    def update_instruction(self, field_name: str, instruction: str) -> None:
        """
        Update the instruction for a field.
        Supports both flat field names and dot-notation paths for nested fields.
        
        Args:
            field_name: Name of the field (can be dot-notation for nested fields)
            instruction: New instruction
        """
        if field_name in self.properties:
            prop = self.properties[field_name]
            if isinstance(prop, SchemaProperty):
                prop.instruction = instruction
            elif isinstance(prop, dict) and "instruction" in prop:
                prop["instruction"] = instruction
    
    def is_nested(self) -> bool:
        """
        Check if this schema contains nested structures.
        
        Returns:
            True if schema contains nested structures, False otherwise
        """
        from src.services.schema_converter import SchemaFlattener
        flattener = SchemaFlattener()
        return flattener.is_nested_schema(self.model_dump(by_alias=True))
    
    def flatten_for_optimization(self) -> Tuple['Schema', Dict[str, str]]:
        """
        Create flattened version for optimization.
        
        Returns:
            Tuple containing:
            - Flattened Schema instance with dot-notation field names
            - Path mapping for reconstruction
        """
        from src.services.schema_converter import SchemaFlattener
        
        flattener = SchemaFlattener()
        schema_dict = self.model_dump(by_alias=True)
        flattened_dict, path_mapping = flattener.flatten_schema(schema_dict)
        
        # Convert flattened properties to SchemaProperty instances
        flattened_properties = {}
        for field_name, prop_def in flattened_dict.get("properties", {}).items():
            try:
                flattened_properties[field_name] = SchemaProperty(**prop_def)
            except Exception as e:
                # If we can't create a SchemaProperty, it means this field is malformed
                # Skip it and continue with other fields
                logger.warning(f"Skipping malformed field '{field_name}': {str(e)}")
                continue
        
        # Create new Schema instance with flattened properties
        flattened_schema = Schema(
            **{k: v for k, v in flattened_dict.items() if k != "properties"},
            properties=flattened_properties
        )
        
        return flattened_schema, path_mapping
    
    def unflatten_from_optimization(self, flat_schema: 'Schema', path_mapping: Dict[str, str]) -> 'Schema':
        """
        Reconstruct nested schema from optimized flat version.
        
        Args:
            flat_schema: Flattened Schema instance with optimized instructions
            path_mapping: Path mapping from flattening operation
            
        Returns:
            Schema instance with original nested structure and optimized instructions
        """
        from src.services.schema_converter import SchemaUnflattener
        
        if not path_mapping:
            # No mapping means it was already flat, return the flat schema
            return flat_schema
        
        unflattener = SchemaUnflattener()
        flat_dict = flat_schema.model_dump(by_alias=True)
        nested_dict = unflattener.unflatten_schema(flat_dict, path_mapping)
        
        # Convert nested properties back to proper structure
        # Note: The unflattened structure may have nested dicts, not SchemaProperty instances
        # We need to preserve the original nested structure
        return Schema(**nested_dict)
