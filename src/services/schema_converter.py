"""
Schema conversion utilities for handling nested blueprint structures.

This module provides functionality to flatten nested schemas into dot-notation paths
for optimization processing, and then reconstruct the original nested structure.
"""
from typing import Dict, Any, Tuple, List
import re


class SchemaFlattener:
    """
    Converts nested blueprint structures to flat field paths using dot notation.
    """
    
    def flatten_schema(self, nested_schema: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        Flatten nested schema to dot-notation paths.
        
        Args:
            nested_schema: The nested schema structure to flatten
            
        Returns:
            Tuple containing:
            - Flattened schema with dot-notation field names
            - Mapping of flat paths to original nested paths for reconstruction
        """
        if not self.is_nested_schema(nested_schema):
            # Return as-is if not nested
            return nested_schema, {}
        
        flattened_properties = {}
        path_mapping = {}
        
        # Store definitions for $ref resolution
        self.definitions = nested_schema.get("definitions", {})
        
        # Process the properties section
        if "properties" in nested_schema:
            self._flatten_properties(
                nested_schema["properties"], 
                "", 
                flattened_properties, 
                path_mapping
            )
        
        # Create flattened schema with same structure but flat properties
        flattened_schema = nested_schema.copy()
        flattened_schema["properties"] = flattened_properties
        
        return flattened_schema, path_mapping
    
    def is_nested_schema(self, schema: Dict[str, Any]) -> bool:
        """
        Check if schema contains nested structures (objects or arrays).
        
        Args:
            schema: Schema to check
            
        Returns:
            True if schema contains nested structures, False otherwise
        """
        if "properties" not in schema:
            return False
        
        return self._has_nested_properties(schema["properties"])
    
    def _has_nested_properties(self, properties: Dict[str, Any]) -> bool:
        """Check if properties contain nested structures."""
        for prop_name, prop_def in properties.items():
            if isinstance(prop_def, dict):
                prop_type = prop_def.get("type", "")
                
                # Check for $ref references to definitions (JSON Schema references)
                if "$ref" in prop_def:
                    return True
                
                # Check for object type with nested properties
                if prop_type == "object" and "properties" in prop_def:
                    return True
                
                # Check for array type with object items
                if prop_type == "array" and "items" in prop_def:
                    items_def = prop_def["items"]
                    if isinstance(items_def, dict):
                        items_type = items_def.get("type", "")
                        if items_type == "object" and "properties" in items_def:
                            return True
        
        return False
    
    def _flatten_properties(self, properties: Dict[str, Any], prefix: str, 
                          flattened: Dict[str, Any], path_mapping: Dict[str, str]) -> None:
        """
        Recursively flatten properties into dot-notation paths.
        
        Args:
            properties: Properties to flatten
            prefix: Current path prefix
            flattened: Dictionary to store flattened properties
            path_mapping: Dictionary to store path mappings for reconstruction
        """
        for prop_name, prop_def in properties.items():
            if not isinstance(prop_def, dict):
                continue
            
            current_path = f"{prefix}.{prop_name}" if prefix else prop_name
            prop_type = prop_def.get("type", "")
            
            # Handle $ref references to definitions
            if "$ref" in prop_def:
                ref_path = prop_def["$ref"]
                if ref_path.startswith("#/definitions/"):
                    def_name = ref_path.replace("#/definitions/", "").replace("%20", " ")
                    
                    # Resolve the definition and flatten its properties
                    if hasattr(self, 'definitions') and def_name in self.definitions:
                        definition = self.definitions[def_name]
                        if isinstance(definition, dict) and "properties" in definition:
                            # Recursively flatten the referenced definition's properties
                            self._flatten_properties(
                                definition["properties"], 
                                current_path, 
                                flattened, 
                                path_mapping
                            )
                        else:
                            # Definition doesn't have properties, treat as simple field
                            flattened[current_path] = prop_def
                            path_mapping[current_path] = self._build_nested_path(current_path)
                    else:
                        # Can't resolve definition, treat as simple field
                        flattened[current_path] = prop_def
                        path_mapping[current_path] = self._build_nested_path(current_path)
                else:
                    # Non-definition $ref, treat as simple field
                    flattened[current_path] = prop_def
                    path_mapping[current_path] = self._build_nested_path(current_path)
            elif prop_type == "object" and "properties" in prop_def:
                # Recursively flatten object properties
                self._flatten_properties(
                    prop_def["properties"], 
                    current_path, 
                    flattened, 
                    path_mapping
                )
            elif prop_type == "array" and "items" in prop_def:
                items_def = prop_def["items"]
                if isinstance(items_def, dict) and items_def.get("type") == "object" and "properties" in items_def:
                    # Handle array of objects - use [*] notation
                    array_path = f"{current_path}[*]"
                    self._flatten_properties(
                        items_def["properties"], 
                        array_path, 
                        flattened, 
                        path_mapping
                    )
                else:
                    # Simple array - treat as flat field
                    flattened[current_path] = prop_def
                    path_mapping[current_path] = self._build_nested_path(current_path)
            else:
                # Simple property - add to flattened structure
                flattened[current_path] = prop_def
                path_mapping[current_path] = self._build_nested_path(current_path)
    
    def _build_nested_path(self, flat_path: str) -> str:
        """
        Build the nested path for reconstruction mapping.
        
        Args:
            flat_path: Flattened dot-notation path
            
        Returns:
            Nested path for reconstruction (not used in current implementation)
        """
        # This method is kept for interface compatibility but not used
        # The unflattener uses the flat path directly for reconstruction
        return flat_path


class SchemaUnflattener:
    """
    Reconstructs nested blueprint structures from optimized flat fields.
    """
    
    def unflatten_schema(self, flat_schema: Dict[str, Any], path_mapping: Dict[str, str]) -> Dict[str, Any]:
        """
        Reconstruct nested schema from flattened paths.
        
        Args:
            flat_schema: Schema with dot-notation field names
            path_mapping: Mapping from flat paths to nested structure
            
        Returns:
            Nested schema with original structure
        """
        if not path_mapping:
            # No mapping means it was already flat
            return flat_schema
        
        # Start with the base schema structure
        nested_schema = {k: v for k, v in flat_schema.items() if k != "properties"}
        nested_properties = {}
        
        # Process each flattened property
        for flat_path, prop_def in flat_schema.get("properties", {}).items():
            self._unflatten_property(flat_path, prop_def, nested_properties)
        
        nested_schema["properties"] = nested_properties
        return nested_schema
    
    def _unflatten_property(self, flat_path: str, prop_def: Dict[str, Any], 
                           nested_properties: Dict[str, Any]) -> None:
        """
        Unflatten a single property back to nested structure.
        
        Args:
            flat_path: Dot-notation path (e.g., "customer.name", "items[*].price")
            prop_def: Property definition
            nested_properties: Dictionary to build nested structure in
        """
        path_parts = self._parse_flat_path(flat_path)
        current_level = nested_properties
        
        for i, part in enumerate(path_parts):
            is_last = (i == len(path_parts) - 1)
            
            if part["type"] == "property":
                prop_name = part["name"]
                
                if is_last:
                    # Last part - add the actual property
                    current_level[prop_name] = prop_def
                else:
                    # Intermediate part - create object structure
                    if prop_name not in current_level:
                        current_level[prop_name] = {
                            "type": "object",
                            "properties": {}
                        }
                    current_level = current_level[prop_name]["properties"]
            
            elif part["type"] == "array":
                prop_name = part["name"]
                
                if prop_name not in current_level:
                    current_level[prop_name] = {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {}
                        }
                    }
                
                if is_last:
                    # This shouldn't happen with proper array notation
                    current_level[prop_name]["items"] = prop_def
                else:
                    # Move to array items properties
                    current_level = current_level[prop_name]["items"]["properties"]
    
    def _parse_flat_path(self, flat_path: str) -> List[Dict[str, str]]:
        """
        Parse a flat path into structured parts.
        
        Args:
            flat_path: Dot-notation path (e.g., "customer.name", "items[*].price")
            
        Returns:
            List of path parts with type information
        """
        parts = []
        segments = flat_path.split('.')
        
        for segment in segments:
            if '[*]' in segment:
                # Array notation
                base_name = segment.replace('[*]', '')
                parts.append({"type": "array", "name": base_name})
            else:
                # Regular property
                parts.append({"type": "property", "name": segment})
        
        return parts