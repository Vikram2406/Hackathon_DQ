"""
Base Agent class for all agentic data quality agents
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import sys
import os
import hashlib
import uuid
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models.schemas import AgenticIssue


class BaseAgent(ABC):
    """Base class for all agentic data quality agents"""
    
    def __init__(self, llm_client=None):
        """
        Initialize agent
        
        Args:
            llm_client: Optional LLM client (OpenAI QueryEngine instance)
        """
        self.llm_client = llm_client
        self.category = self.__class__.__name__.replace('Agent', '').title()
    
    @abstractmethod
    def run(
        self,
        dataset_rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        llm_client=None
    ) -> List[AgenticIssue]:
        """
        Run agent on dataset rows
        
        Args:
            dataset_rows: List of row dictionaries (column -> value)
            metadata: Dataset metadata (schema, column types, etc.)
            llm_client: Optional LLM client override
            
        Returns:
            List of AgenticIssue objects
        """
        pass
    
    def _create_issue(
        self,
        row_id: Optional[int],
        column: str,
        issue_type: str,
        dirty_value: Any,
        suggested_value: Any,
        confidence: float,
        explanation: str,
        why_agentic: Optional[str] = None
    ) -> AgenticIssue:
        """Helper to create an AgenticIssue with a guaranteed unique ID"""
        # Generate a short UUID for absolute uniqueness
        # This ensures no collisions even if same issue is created multiple times
        unique_id = str(uuid.uuid4())[:8]
        
        # Generate unique issue ID with UUID
        issue_id = f"{self.category}_{issue_type}_{row_id or 'dataset'}_{column}_{unique_id}"
        
        return AgenticIssue(
            id=issue_id,
            row_id=row_id,
            column=column,
            category=self.category,
            issue_type=issue_type,
            dirty_value=dirty_value,
            suggested_value=suggested_value,
            confidence=confidence,
            explanation=explanation,
            why_agentic=why_agentic
        )