from pydantic import BaseModel, Field, ConfigDict, ValidationError

class User(BaseModel):
    """
    User model representing a user in the system.
    """
    id: int = Field(..., description="The unique identifier for the user")
    name: str = Field(..., description="The name of the user")
    email: str = Field(..., description="The email address of the user")

    model_config = ConfigDict(
        extra='forbid',  # Forbid extra fields not defined in the model
        validate_assignment=True  # Validate data on assignment
    )