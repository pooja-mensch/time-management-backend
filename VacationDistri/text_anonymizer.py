"""
Text Anonymizer Module
Handles anonymization of text content using spaCy NLP.
"""

import spacy
import logging
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any
import copy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnonymizationError(Exception):
    """Custom exception for anonymization errors"""
    pass


class TextAnonymizer:
    """Handles anonymization of text using Named Entity Recognition."""
    
    DEFAULT_MODELS = {
        'german': 'de_core_news_sm',
        'english': 'en_core_web_sm'
    }
    
    def __init__(self, spacy_model: str = "de_core_news_sm"):
        """
        Initialize the anonymizer with a spaCy model.
        
        Args:
            spacy_model: spaCy model name (default: German model)
        """
        self.spacy_model = spacy_model
        self.nlp = None
        self.mapping_dict = {}
        self.entity_to_label = {}
        self.counter = defaultdict(int)
        self.session_stats = {
            'total_entities': 0,
            'entity_types': defaultdict(int),
            'unique_entities': 0
        }
        
        self._load_spacy_model()
    
    def _load_spacy_model(self):
        """Load the spaCy model with proper error handling."""
        try:
            self.nlp = spacy.load(self.spacy_model)
            logger.info(f"Successfully loaded spaCy model: {self.spacy_model}")
        except OSError:
            logger.error(f"spaCy model '{self.spacy_model}' not found")
            logger.info("Install with: python -m spacy download de_core_news_sm")
            logger.info("Or use: python -m spacy download en_core_web_sm for English")
            self.nlp = None
    
    def is_available(self) -> bool:
        """Check if anonymization is available."""
        return self.nlp is not None
    
    def anonymize_text(self, text: str) -> Tuple[str, Dict[str, str]]:
        """
        Anonymize text by replacing named entities with generic labels.
        
        Args:
            text: Input text to anonymize
            
        Returns:
            Tuple of (anonymized_text, local_mapping_dict)
        """
        if not self.nlp:
            logger.warning("spaCy not available, returning original text")
            return text, {}
        
        if not text or not text.strip():
            return text, {}
        
        try:
            doc = self.nlp(text)
            local_mapping = {}
            
            # Collect entities with their positions (sorted by position, reversed for replacement)
            entities = []
            for ent in doc.ents:
                entities.append({
                    'start': ent.start_char,
                    'end': ent.end_char,
                    'text': ent.text,
                    'label': ent.label_
                })
            
            # Sort by start position (reversed for safe replacement)
            entities.sort(key=lambda x: x['start'], reverse=True)
            
            # Process entities
            anonymized_text = text
            for entity in entities:
                ent_text = entity['text']
                ent_label = entity['label']
                start_pos = entity['start']
                end_pos = entity['end']
                
                # Get or create anonymized label
                if ent_text in self.entity_to_label:
                    anon_label = self.entity_to_label[ent_text]
                else:
                    self.counter[ent_label] += 1
                    anon_label = f"{ent_label}_{self.counter[ent_label]}"
                    self.entity_to_label[ent_text] = anon_label
                    self.mapping_dict[anon_label] = ent_text
                    self.session_stats['unique_entities'] += 1
                
                # Update local mapping
                local_mapping[anon_label] = ent_text
                
                # Replace entity in text
                anonymized_text = anonymized_text[:start_pos] + anon_label + anonymized_text[end_pos:]
                
                # Update session stats
                self.session_stats['total_entities'] += 1
                self.session_stats['entity_types'][ent_label] += 1
            
            return anonymized_text, local_mapping
            
        except Exception as e:
            logger.error(f"Text anonymization failed: {str(e)}")
            return text, {}
    
    def anonymize_extracted_content(self, extraction_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Anonymize content from document extraction results.
        
        Args:
            extraction_results: Results from DocumentExtractor
            
        Returns:
            Anonymized results with original structure preserved
        """
        if not self.is_available():
            logger.warning("Anonymization not available, returning original results")
            return extraction_results
        
        logger.info("Starting content anonymization...")
        
        # Create deep copy of results
        anonymized_results = copy.deepcopy(extraction_results)
        
        # Add anonymization metadata
        anonymized_results['anonymization_metadata'] = {
            'anonymized': True,
            'spacy_model': self.spacy_model,
            'total_mappings': 0,
            'entity_types_found': []
        }
        
        try:
            if extraction_results['file_type'] == 'pdf':
                self._anonymize_pdf_content(anonymized_results)
            elif extraction_results['file_type'] == 'excel':
                self._anonymize_excel_content(anonymized_results)
            
            # Update metadata
            anonymized_results['anonymization_metadata']['total_mappings'] = len(self.mapping_dict)
            anonymized_results['anonymization_metadata']['entity_types_found'] = list(self.session_stats['entity_types'].keys())
            
            logger.info(f"Anonymization completed: {len(self.mapping_dict)} entities anonymized")
            
        except Exception as e:
            logger.error(f"Anonymization process failed: {str(e)}")
            raise AnonymizationError(f"Failed to anonymize content: {str(e)}")
        
        return anonymized_results
    
    def _anonymize_pdf_content(self, results: Dict[str, Any]):
        """Anonymize PDF content in-place."""
        logger.info("Anonymizing PDF pages...")
        
        for page in results['pages']:
            page_num = page['page_number']
            
            # Anonymize text content
            if page['text_content']:
                anon_text, local_mapping = self.anonymize_text(page['text_content'])
                page['text_content'] = anon_text
                page['anonymization_mapping'] = local_mapping
                
                if local_mapping:
                    logger.debug(f"Page {page_num}: {len(local_mapping)} entities anonymized in text")
            
            # Anonymize table content
            for table in page['tables']:
                table_mappings = {}
                for row_idx, row in enumerate(table['data']):
                    for col_name, cell_value in row.items():
                        if cell_value and isinstance(cell_value, str):
                            anon_cell, cell_mapping = self.anonymize_text(str(cell_value))
                            table['data'][row_idx][col_name] = anon_cell
                            table_mappings.update(cell_mapping)
                
                table['anonymization_mapping'] = table_mappings
    
    def _anonymize_excel_content(self, results: Dict[str, Any]):
        """Anonymize Excel content in-place."""
        logger.info("Anonymizing Excel sheets...")
        
        for sheet in results['sheets']:
            sheet_name = sheet['sheet_name']
            logger.debug(f"Anonymizing sheet: {sheet_name}")
            
            sheet_mappings = {}
            for row_idx, row_data in enumerate(sheet['content']):
                # Anonymize raw text
                anon_text, local_mapping = self.anonymize_text(row_data['raw_text'])
                sheet['content'][row_idx]['raw_text'] = anon_text
                sheet_mappings.update(local_mapping)
                
                # Anonymize individual cells
                anon_cells = []
                for cell in row_data['cells']:
                    if isinstance(cell, str):
                        anon_cell, cell_mapping = self.anonymize_text(cell)
                        anon_cells.append(anon_cell)
                        sheet_mappings.update(cell_mapping)
                    else:
                        anon_cells.append(cell)
                
                sheet['content'][row_idx]['cells'] = anon_cells
            
            sheet['anonymization_mapping'] = sheet_mappings
    
    def reverse_anonymization(self, anonymized_text: str, mapping: Optional[Dict[str, str]] = None) -> str:
        """
        Reverse anonymization using the mapping dictionary.
        
        Args:
            anonymized_text: Text with anonymized entities
            mapping: Optional specific mapping dict (uses global mapping if None)
            
        Returns:
            Text with entities restored to original form
        """
        if mapping is None:
            mapping = self.mapping_dict
        
        reversed_text = anonymized_text
        
        # Sort by label length (longest first) to avoid partial replacements
        sorted_labels = sorted(mapping.keys(), key=len, reverse=True)
        
        for anon_label in sorted_labels:
            original_text = mapping[anon_label]
            reversed_text = reversed_text.replace(anon_label, original_text)
        
        return reversed_text
    
    def get_anonymization_stats(self) -> Dict[str, Any]:
        """Get statistics about the anonymization process."""
        return {
            'total_entities': self.session_stats['total_entities'],
            'entity_types': dict(self.session_stats['entity_types']),
            'unique_entities': self.session_stats['unique_entities'],
            'total_mappings': len(self.mapping_dict),
            'spacy_model': self.spacy_model,
            'available': self.is_available()
        }
    
    def reset_session(self):
        """Reset the anonymization session (clear all mappings and counters)."""
        self.mapping_dict = {}
        self.entity_to_label = {}
        self.counter = defaultdict(int)
        self.session_stats = {
            'total_entities': 0,
            'entity_types': defaultdict(int),
            'unique_entities': 0
        }
        logger.info("Anonymizer session reset")
    
    def prepare_data_for_llm(self, anonymized_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare anonymized data for LLM processing by removing mapping information.
        This ensures that original entity information is not leaked to the LLM.
        
        Args:
            anonymized_data: Anonymized data dictionary
            
        Returns:
            Clean data dictionary without mapping information
        """
        clean_data = copy.deepcopy(anonymized_data)
        
        # Remove global mapping if exists
        clean_data.pop("anonymization_mapping", None)
        
        # Remove per-page/sheet mappings
        if 'pages' in clean_data:
            for page in clean_data['pages']:
                page.pop("anonymization_mapping", None)
                for table in page.get('tables', []):
                    table.pop("anonymization_mapping", None)
        
        if 'sheets' in clean_data:
            for sheet in clean_data['sheets']:
                sheet.pop("anonymization_mapping", None)
        
        return clean_data