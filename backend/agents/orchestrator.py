"""
Agents Orchestrator - Coordinates all agentic data quality agents
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict, Any, Optional
from models.schemas import AgenticIssue
from agents.base_agent import BaseAgent
from agents.formatting import FormattingAgent
from agents.units import UnitsAgent
from agents.categorical import CategoricalAgent
from agents.imputation import ImputationAgent
from agents.semantic import SemanticAgent
from agents.logic import LogicAgent
from agents.extraction import ExtractionAgent
from agents.email_validation import EmailValidationAgent
from agents.company_validation import CompanyValidationAgent
from agents.geographic_enrichment import GeographicEnrichmentAgent


class AgentsOrchestrator:
    """Orchestrates running all agentic data quality agents on a dataset"""
    
    def __init__(self, llm_client=None):
        """
        Initialize orchestrator
        
        Args:
            llm_client: Optional LLM client (OpenAI QueryEngine instance)
        """
        self.llm_client = llm_client
        self.agents: List[BaseAgent] = [
            EmailValidationAgent(llm_client=llm_client),
            GeographicEnrichmentAgent(llm_client=llm_client),  # Run BEFORE FormattingAgent so country is inferred first
            FormattingAgent(llm_client=llm_client),  # Can now use country from geographic enrichment
            CompanyValidationAgent(llm_client=llm_client),
            UnitsAgent(llm_client=llm_client),
            CategoricalAgent(llm_client=llm_client),
            ImputationAgent(llm_client=llm_client),
            SemanticAgent(llm_client=llm_client),
            LogicAgent(llm_client=llm_client),
            ExtractionAgent(llm_client=llm_client),
        ]
    
    def run(
        self,
        validation_result: Dict[str, Any],
        dataset_rows: Optional[List[Dict[str, Any]]] = None,
        sample_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Run all agents on a validation result
        
        Args:
            validation_result: Validation result JSON from S3 (contains dataset, row_count, results, summary)
            dataset_rows: Optional full dataset rows. If None, will try to load sample from S3
            sample_size: If dataset_rows not provided, sample this many rows
            
        Returns:
            Dict with 'agentic_issues' (list) and 'agentic_summary' (dict)
        """
        # Extract metadata from validation result
        metadata = {
            'dataset': validation_result.get('dataset', ''),
            'source': validation_result.get('source', ''),
            'row_count': validation_result.get('row_count', 0),
            'timestamp': validation_result.get('timestamp', ''),
            'schema': validation_result.get('schema_definition', {}),
            'results': validation_result.get('results', {}),
            'summary': validation_result.get('summary', {})
        }
        
        # If no rows provided, we'll work with metadata only (agents can still detect issues)
        if dataset_rows is None:
            dataset_rows = []
            # Try to load sample from S3 if possible
            # For now, agents will work with metadata/column info only
        
        # Run all agents with progress indication
        all_issues: List[AgenticIssue] = []
        total_agents = len(self.agents)
        
        for idx, agent in enumerate(self.agents, 1):
            try:
                print(f"🔄 Running agent {idx}/{total_agents}: {agent.__class__.__name__}...")
                issues = agent.run(dataset_rows, metadata, self.llm_client)
                all_issues.extend(issues)
                print(f"   ✅ {agent.__class__.__name__}: Found {len(issues)} issues")
                if issues:
                    # Debug: Show issue types found
                    issue_types = {}
                    for issue in issues:
                        issue_type = issue.issue_type
                        issue_types[issue_type] = issue_types.get(issue_type, 0) + 1
                    print(f"      Issue types: {issue_types}")
            except Exception as e:
                print(f"⚠️ Error running {agent.__class__.__name__}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Generate summary
        summary = self._generate_summary(all_issues, metadata)
        
        # Convert issues to dict format, ensuring all fields are serializable
        issues_dict = []
        for issue in all_issues:
            try:
                issue_dict = issue.model_dump()
                # Ensure all values are JSON-serializable
                if issue_dict.get('dirty_value') is None:
                    issue_dict['dirty_value'] = ''
                if issue_dict.get('suggested_value') is None:
                    issue_dict['suggested_value'] = ''
                issues_dict.append(issue_dict)
            except Exception as e:
                print(f"⚠️ Error serializing issue: {e}, issue: {issue}")
                # Try manual conversion
                try:
                    issues_dict.append({
                        'id': issue.id,
                        'row_id': issue.row_id,
                        'column': issue.column,
                        'category': issue.category,
                        'issue_type': issue.issue_type,
                        'dirty_value': str(issue.dirty_value) if issue.dirty_value is not None else '',
                        'suggested_value': str(issue.suggested_value) if issue.suggested_value is not None else '',
                        'confidence': issue.confidence,
                        'explanation': issue.explanation,
                        'why_agentic': issue.why_agentic
                    })
                except Exception as e2:
                    print(f"❌ Failed to serialize issue: {e2}")
                    continue
        
        print(f"DEBUG: Orchestrator returning {len(issues_dict)} issues")
        categories = {}
        for issue in issues_dict:
            cat = issue.get('category', 'N/A')
            categories[cat] = categories.get(cat, 0) + 1
        print(f"DEBUG: Categories breakdown: {categories}")
        print(f"DEBUG: Total unique categories: {len(categories)}")
        
        return {
            'agentic_issues': issues_dict,
            'agentic_summary': summary
        }
    
    def _generate_summary(
        self,
        issues: List[AgenticIssue],
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate summary statistics from issues"""
        total_rows = metadata.get('row_count', 0)
        total_issues = len(issues)
        
        # Count by category
        category_counts = {}
        issue_type_counts = {}
        
        for issue in issues:
            category = issue.category
            issue_type = issue.issue_type
            
            category_counts[category] = category_counts.get(category, 0) + 1
            issue_type_counts[issue_type] = issue_type_counts.get(issue_type, 0) + 1
        
        # Calculate rows affected
        rows_affected = len(set(issue.row_id for issue in issues if issue.row_id is not None))
        rows_affected_percent = (rows_affected / total_rows * 100) if total_rows > 0 else 0
        
        return {
            'total_rows_scanned': total_rows,
            'total_issues': total_issues,
            'rows_affected': rows_affected,
            'rows_affected_percent': round(rows_affected_percent, 2),
            'category_counts': category_counts,
            'issue_type_counts': issue_type_counts
        }