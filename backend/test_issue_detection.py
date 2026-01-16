"""
Test script to diagnose why issues aren't appearing in the matrix
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env file if it exists
from dotenv import load_dotenv
load_dotenv()

# Ensure LLM_PROVIDER is set (defaults to openai if not set)
if not os.getenv('LLM_PROVIDER'):
    os.environ['LLM_PROVIDER'] = 'openai'

from agents.llm_provider import LLMProviderFactory
from agents.orchestrator import AgentsOrchestrator

# Test data with various issues
test_rows = [
    {'email': 'vik@gmail', 'phone': '7083990477', 'company': 'MS', 'city': 'Pune', 'state': 'Washington', 'country': 'India', 'height': '1.78 meters'},
    {'email': 'test@', 'phone': '1234567890', 'company': 'Microsoft', 'city': 'Mumbai', 'state': 'Florida', 'country': None, 'height': '5 feet'},
    {'email': 'valid@email.com', 'phone': '+91 9876543210', 'company': 'TCS', 'city': 'Bangalore', 'state': None, 'country': None, 'height': '180 cm'},
]

print("🧪 Testing complete issue detection flow...\n")

# Initialize orchestrator
llm = LLMProviderFactory.create_llm_client()
orchestrator = AgentsOrchestrator(llm_client=llm)

# Create mock validation result
validation_result = {
    'dataset': 'test_dataset',
    'row_count': len(test_rows),
    'results': {},
    'summary': {}
}

# Run orchestrator
print("Running orchestrator...")
result = orchestrator.run(
    validation_result=validation_result,
    dataset_rows=test_rows
)

print(f"\n✅ Orchestrator completed")
print(f"   Total issues: {len(result['agentic_issues'])}")

# Check categories
categories = {}
for issue in result['agentic_issues']:
    cat = issue.get('category', 'Unknown')
    issue_type = issue.get('issue_type', 'Unknown')
    key = f"{cat}/{issue_type}"
    categories[key] = categories.get(key, 0) + 1

print(f"\n📊 Issue breakdown:")
for key, count in sorted(categories.items()):
    print(f"   {key}: {count}")

# Check if GeographicEnrichment is present
geo_issues = [i for i in result['agentic_issues'] if i.get('category') == 'GeographicEnrichment']
print(f"\n🌍 GeographicEnrichment issues: {len(geo_issues)}")
if geo_issues:
    for issue in geo_issues:
        print(f"   - {issue.get('issue_type')} in {issue.get('column')}: '{issue.get('dirty_value')}' -> '{issue.get('suggested_value')}'")
else:
    print("   ⚠️ NO GEOGRAPHIC ISSUES FOUND!")

# Test matrix building
print(f"\n📋 Testing matrix building...")
from models.schemas import AgenticIssue, AgenticIssueSummary

matrix_dict = {}
for issue_dict in result['agentic_issues']:
    try:
        issue = AgenticIssue(**issue_dict)
        key = (issue.category, issue.issue_type)
        
        if key not in matrix_dict:
            matrix_dict[key] = {
                'category': issue.category,
                'issue_type': issue.issue_type,
                'count': 0,
                'dirty_example': str(issue.dirty_value)[:50] if issue.dirty_value is not None else 'N/A',
                'smart_fix_example': str(issue.suggested_value)[:50] if issue.suggested_value is not None else 'N/A',
                'why_agentic': issue.why_agentic or issue.explanation or 'AI-Powered'
            }
        
        matrix_dict[key]['count'] += 1
    except Exception as e:
        print(f"   ❌ Error: {e}")

matrix = [AgenticIssueSummary(**v) for v in matrix_dict.values()]
print(f"   Matrix entries: {len(matrix)}")
for m in matrix:
    print(f"   - {m.category} / {m.issue_type}: {m.count}")
