"""
Configuration models for the BDA optimization application.
"""
from typing import List, Optional
from pydantic import BaseModel, Field


class InputField(BaseModel):
    """
    Represents a field in the input data that needs to be extracted.
    """
    instruction: str = Field(description="The instruction for extracting this field")
    data_point_in_document: bool = Field(description="Whether this field exists in the document")
    field_name: str = Field(description="The name of the field to extract")
    expected_output: str = Field(description="The expected output for this field")


class BDAConfig(BaseModel):
    """
    Configuration for the BDA optimization process.
    """
    project_arn: str = Field(description="ARN of the project")
    blueprint_id: str = Field(description="ID of the blueprint")
    dataAutomation_profilearn: str = Field(description="ARN of the data automation profile")
    project_stage: str = Field(description="Stage of the project (e.g., 'LIVE')")
    input_document: str = Field(description="S3 URI for the input document")
    inputs: List[InputField] = Field(description="List of fields to extract")

    @classmethod
    def from_file(cls, file_path: str) -> "BDAConfig":
        """
        Load configuration from a JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            BDAConfig: Loaded configuration
        """
        import json
        with open(file_path, 'r') as f:
            data = json.load(f)
        return cls(**data)
    
    def to_file(self, file_path: str) -> None:
        """
        Save configuration to a JSON file.
        
        Args:
            file_path: Path to save the JSON file
        """
        import json
        with open(file_path, 'w') as f:
            json.dump(self.model_dump(), f, indent=2)
