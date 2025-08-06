"""
LLM Data Restructuring Module
Takes anonymized JSON data and uses LLM to restructure and organize it.
"""

import json
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import tiktoken
except ImportError:
    tiktoken = None
    logger.warning("tiktoken not available - token counting will use fallback estimation")


class LLMError(Exception):
    """Custom exception for LLM-related errors"""
    pass


class LLMDataRestructurer:
    """Uses LLM to restructure and organize anonymized document data."""
    
    DEFAULT_CONFIG = {
        'base_url': 'http://localhost:1234/v1/chat/completions',
        'model': 'phi-3-mini-4k-instruct',
        'temperature': 0.1,
        'max_tokens': 4000,
        'timeout': 120
    }
    
    def __init__(self, 
                 model: str = None,
                 base_url: str = None,
                 api_key: Optional[str] = None):
        """
        Initialize the LLM restructurer.
        
        Args:
            model: LLM model name
            base_url: LLM API endpoint URL
            api_key: API key (if required)
        """
        self.model = model or self.DEFAULT_CONFIG['model']
        self.base_url = base_url or self.DEFAULT_CONFIG['base_url']
        self.api_key = api_key
        self.request_config = {
            'temperature': self.DEFAULT_CONFIG['temperature'],
            'max_tokens': self.DEFAULT_CONFIG['max_tokens'],
            'timeout': self.DEFAULT_CONFIG['timeout']
        }
        
    def is_available(self) -> bool:
        """Check if LLM service is available by testing connectivity."""
        try:
            response = requests.get(
                self.base_url.replace('/chat/completions', '/health'),
                timeout=5
            )
            return response.status_code == 200
        except:
            # Fallback: just check if base_url is configured
            return self.base_url is not None
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate token count for the given text."""
        if tiktoken:
            try:
                enc = tiktoken.encoding_for_model(self.model.strip())
            except KeyError:
                enc = tiktoken.get_encoding("cl100k_base")
            return len(enc.encode(text))
        else:
            # Fallback: rough estimation of 4 characters per token
            return len(text) // 4
    
    def create_restructuring_prompt(self, anonymized_data: Dict[str, Any]) -> str:
        """Create an appropriate prompt based on file type."""
        file_type = anonymized_data.get('file_type', 'unknown')
        
        if file_type == 'excel':
            return self._create_excel_prompt(anonymized_data)
        elif file_type == 'pdf':
            return self._create_pdf_prompt(anonymized_data)
        else:
            return self._create_generic_prompt(anonymized_data)
    
    def _create_excel_prompt(self, data: Dict[str, Any]) -> str:
        """Create a prompt specifically for Excel absence/vacation data."""
        return f"""You are a data structuring expert. Transform the following anonymized Excel data into a structured JSON format for employee absence records.

IMPORTANT: Use real values extracted from the data, not placeholders. Replace:
- "number" → actual numbers like 5, 10, etc.
- "YYYY-MM-DD" → actual dates like "2023-01-15"
- "vacation|sick_leave|other" → specific types like "vacation"

Expected JSON structure:
{{
    "document_type": "absence_records",
    "extraction_date": "{datetime.now().isoformat()}",
    "data_period": {{
        "start_date": "earliest_date_found",
        "end_date": "latest_date_found"
    }},
    "employees": [
        {{
            "employee_id": "generated_id_or_anonymized_name",
            "employee_name": "anonymized_label_from_data",
            "absence_records": [
                {{
                    "absence_type": "vacation_or_sick_or_other",
                    "start_date": "actual_start_date",
                    "end_date": "actual_end_date",
                    "days": actual_number_of_days,
                    "notes": "any_additional_information"
                }}
            ],
            "summary": {{
                "total_vacation_days": actual_number,
                "total_sick_days": actual_number,
                "total_other_days": actual_number
            }}
        }}
    ],
    "organization_summary": {{
        "total_employees": actual_count,
        "total_vacation_days": sum_of_all_vacation,
        "total_sick_days": sum_of_all_sick,
        "total_other_days": sum_of_all_other
    }}
}}

Rules:
1. Extract ALL employee records from the data
2. Categorize absences into: vacation, sick_leave, or other
3. Calculate accurate totals for each category
4. Preserve anonymized labels exactly as they appear
5. Use actual dates in YYYY-MM-DD format
6. Return ONLY valid JSON, no additional text

Data to process:
{json.dumps(data, indent=2, ensure_ascii=False)}"""

    def _create_pdf_prompt(self, data: Dict[str, Any]) -> str:
        """Create a prompt for PDF document data."""
        return f"""You are a data structuring expert. Transform the following anonymized PDF data into structured JSON.

IMPORTANT: Use actual values from the data, not placeholders.

For absence/vacation records, use this structure:
{{
    "document_type": "absence_records",
    "extraction_date": "{datetime.now().isoformat()}",
    "pages_processed": {len(data.get('pages', []))},
    "employees": [
        {{
            "employee_id": "generated_or_extracted",
            "employee_name": "anonymized_label",
            "absence_records": [
                {{
                    "absence_type": "specific_type",
                    "start_date": "actual_date",
                    "end_date": "actual_date",
                    "days": actual_number,
                    "notes": "extracted_notes"
                }}
            ]
        }}
    ]
}}

For other document types, create appropriate structure based on content.

Rules:
1. Analyze content to determine document type
2. Extract structured data from tables and text
3. Preserve anonymized labels exactly
4. Use YYYY-MM-DD date format
5. Return ONLY valid JSON

Data to process:
{json.dumps(data, indent=2, ensure_ascii=False)}"""
    
    def _create_generic_prompt(self, data: Dict[str, Any]) -> str:
        """Create a generic prompt for unknown document types."""
        return f"""You are a data structuring expert. Analyze and restructure the following anonymized document data.

IMPORTANT: 
- Use actual values from the data, not type placeholders
- Preserve all anonymized labels exactly as they appear
- Structure data logically based on content type
- Use proper date formats (YYYY-MM-DD)
- Return ONLY valid JSON

Data to analyze:
{json.dumps(data, indent=2, ensure_ascii=False)}"""
    
    def restructure_data(self, anonymized_data: Dict[str, Any], max_retries: int = 3) -> Dict[str, Any]:
        """
        Use LLM to restructure the anonymized data.
        
        Args:
            anonymized_data: The anonymized JSON data
            max_retries: Maximum number of retry attempts
            
        Returns:
            Restructured data as dictionary
        """
        if not self.is_available():
            raise LLMError("LLM service not available. Check base_url configuration.")
        
        logger.info("Starting LLM data restructuring...")
        
        prompt = self.create_restructuring_prompt(anonymized_data)
        
        # Log prompt diagnostics
        prompt_length = len(prompt)
        estimated_tokens = self.estimate_tokens(prompt)
        logger.info(f"Prompt length: {prompt_length} characters, ~{estimated_tokens} tokens")
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{max_retries}")
                
                payload = {
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system", 
                            "content": "You are a data structuring expert. Always return valid JSON only, no additional text."
                        },
                        {
                            "role": "user", 
                            "content": prompt
                        }
                    ],
                    "temperature": self.request_config['temperature'],
                    "max_tokens": self.request_config['max_tokens']
                }
                
                headers = {"Content-Type": "application/json"}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"
                
                response = requests.post(
                    self.base_url, 
                    json=payload, 
                    headers=headers, 
                    timeout=self.request_config['timeout']
                )
                response.raise_for_status()
                
                data = response.json()
                response_text = data["choices"][0]["message"]["content"].strip()
                
                # Clean up response text
                if response_text.startswith("```json"):
                    response_text = response_text[7:]
                if response_text.endswith("```"):
                    response_text = response_text[:-3]
                response_text = response_text.strip()
                
                # Parse JSON response
                try:
                    restructured_data = json.loads(response_text)
                    
                    # Add metadata
                    restructured_data['restructuring_metadata'] = {
                        'restructured_by': 'llm',
                        'model_used': self.model,
                        'restructuring_date': datetime.now().isoformat(),
                        'original_file': anonymized_data.get('file_path', ''),
                        'anonymization_preserved': True
                    }
                    
                    logger.info("Data restructuring completed successfully")
                    return restructured_data
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON response: {e}")
                    logger.debug(f"Response: {response_text[:200]}...")
                    
                    if attempt < max_retries - 1:
                        continue
                    else:
                        raise LLMError(f"Failed to get valid JSON after {max_retries} attempts")
                        
            except requests.RequestException as e:
                logger.error(f"LLM request failed: {e}")
                
                if attempt < max_retries - 1:
                    continue
                else:
                    raise LLMError(f"LLM service unavailable after {max_retries} attempts: {e}")
        
        raise LLMError("Unexpected error in restructuring process")
    
    def validate_restructured_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate that the restructured data has expected structure.
        
        Args:
            data: Restructured data to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['document_type', 'extraction_date']
        
        for field in required_fields:
            if field not in data:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Additional validation based on document type
        doc_type = data.get('document_type')
        
        if doc_type == 'absence_records':
            if 'employees' not in data:
                logger.warning("Absence records missing 'employees' field")
                return False
            
            for employee in data.get('employees', []):
                if not isinstance(employee.get('absence_records'), list):
                    logger.warning("Employee missing valid 'absence_records'")
                    return False
        
        return True