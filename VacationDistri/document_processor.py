"""
Document Processor - Integration Module
Orchestrates the complete document processing pipeline.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

# Import our modules (make sure these match your actual file names)
from document_extractor import DocumentExtractor, ExtractionError
from text_anonymizer import TextAnonymizer, AnonymizationError
from llm_restructurer import LLMDataRestructurer, LLMError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessingError(Exception):
    """Custom exception for processing pipeline errors"""
    pass


class DocumentProcessor:
    """
    Main processor that orchestrates document extraction, anonymization, and LLM restructuring.
    """
    
    def __init__(self, 
                 spacy_model: str = "de_core_news_sm",
                 llm_base_url: str = None,
                 llm_model: str = "phi-3-mini-4k-instruct"):
        """
        Initialize the document processor.
        
        Args:
            spacy_model: spaCy model for anonymization
            llm_base_url: Base URL for LLM API
            llm_model: LLM model to use for restructuring
        """
        self.extractor = DocumentExtractor()
        self.anonymizer = TextAnonymizer(spacy_model)
        self.restructurer = LLMDataRestructurer(
            model=llm_model,
            base_url=llm_base_url
        )
        
        self.processing_stats = {
            'files_processed': 0,
            'extraction_errors': 0,
            'anonymization_errors': 0,
            'restructuring_errors': 0,
            'last_processed': None
        }
    
    def get_service_status(self) -> Dict[str, Any]:
        """Get the status of all processing services."""
        return {
            'extractor': {
                'available': True,
                'supported_formats': list(self.extractor.SUPPORTED_FORMATS.keys())
            },
            'anonymizer': {
                'available': self.anonymizer.is_available(),
                'model': self.anonymizer.spacy_model,
                'stats': self.anonymizer.get_anonymization_stats()
            },
            'restructurer': {
                'available': self.restructurer.is_available(),
                'model': self.restructurer.model,
                'base_url': self.restructurer.base_url
            },
            'processing_stats': self.processing_stats
        }
    
    def process_document(self, 
                        file_path: str, 
                        password: Optional[str] = None,
                        anonymize: bool = True,
                        restructure: bool = True) -> Dict[str, Any]:
        """
        Process a document through the complete pipeline.
        
        Args:
            file_path: Path to the document
            password: Optional password for encrypted files
            anonymize: Whether to anonymize the extracted content
            restructure: Whether to use LLM to restructure the data
        
        Returns:
            Dictionary containing all processing results
        """
        logger.info(f"Starting document processing: {os.path.basename(file_path)}")
        
        processing_result = {
            'file_path': file_path,
            'processing_timestamp': datetime.now().isoformat(),
            'phases_completed': [],
            'errors': [],
            'warnings': [],
            'data': None
        }
        
        try:
            # Phase 1: Content Extraction
            logger.info("Phase 1: Content Extraction")
            try:
                extraction_results = self.extractor.extract(file_path, password)
                processing_result['phases_completed'].append('extraction')
                processing_result['data'] = extraction_results
                
                summary = self.extractor.get_extraction_summary(extraction_results)
                logger.info(f"Extraction completed: {summary}")
                
            except ExtractionError as e:
                error_msg = f"Extraction failed: {str(e)}"
                processing_result['errors'].append(error_msg)
                self.processing_stats['extraction_errors'] += 1
                raise ProcessingError(error_msg)
            
            # Phase 2: Content Anonymization
            if anonymize:
                logger.info("Phase 2: Content Anonymization")
                try:
                    if self.anonymizer.is_available():
                        anonymized_results = self.anonymizer.anonymize_extracted_content(extraction_results)
                        processing_result['phases_completed'].append('anonymization')
                        processing_result['data'] = anonymized_results
                        
                        stats = self.anonymizer.get_anonymization_stats()
                        logger.info(f"Anonymization completed: {stats['total_entities']} entities anonymized")
                    else:
                        warning_msg = "Anonymization not available (spaCy model not loaded)"
                        processing_result['warnings'].append(warning_msg)
                        logger.warning(warning_msg)
                        
                except AnonymizationError as e:
                    error_msg = f"Anonymization failed: {str(e)}"
                    processing_result['errors'].append(error_msg)
                    processing_result['warnings'].append("Continuing with non-anonymized data")
                    self.processing_stats['anonymization_errors'] += 1
                    logger.error(error_msg)
            else:
                logger.info("Skipping anonymization (disabled)")
            
            # Phase 3: LLM Data Restructuring
            if restructure:
                logger.info("Phase 3: LLM Data Restructuring")
                try:
                    if self.restructurer.is_available():
                        # Prepare data for LLM (remove mapping info if anonymized)
                        llm_input_data = processing_result['data']
                        if anonymize and self.anonymizer.is_available():
                            llm_input_data = self.anonymizer.prepare_data_for_llm(llm_input_data)
                        
                        restructured_results = self.restructurer.restructure_data(llm_input_data)
                        
                        # Validate restructured data
                        if self.restructurer.validate_restructured_data(restructured_results):
                            processing_result['phases_completed'].append('restructuring')
                            processing_result['restructured_data'] = restructured_results
                            logger.info("LLM restructuring completed and validated")
                        else:
                            warning_msg = "LLM restructuring produced invalid data structure"
                            processing_result['warnings'].append(warning_msg)
                            logger.warning(warning_msg)
                    else:
                        warning_msg = "LLM restructuring not available (service unavailable)"
                        processing_result['warnings'].append(warning_msg)
                        logger.warning(warning_msg)
                        
                except LLMError as e:
                    error_msg = f"LLM restructuring failed: {str(e)}"
                    processing_result['errors'].append(error_msg)
                    processing_result['warnings'].append("Continuing without restructured data")
                    self.processing_stats['restructuring_errors'] += 1
                    logger.error(error_msg)
            else:
                logger.info("Skipping LLM restructuring (disabled)")
            
            # Update stats
            self.processing_stats['files_processed'] += 1
            self.processing_stats['last_processed'] = datetime.now().isoformat()
            
            logger.info(f"Document processing completed: {len(processing_result['phases_completed'])} phases")
            return processing_result
            
        except ProcessingError:
            raise
        except Exception as e:
            error_msg = f"Unexpected processing error: {str(e)}"
            logger.error(error_msg)
            processing_result['errors'].append(error_msg)
            raise ProcessingError(error_msg)
    
    def process_multiple_documents(self, 
                                 file_paths: List[str], 
                                 anonymize: bool = True,
                                 restructure: bool = True) -> List[Dict[str, Any]]:
        """
        Process multiple documents in batch.
        
        Args:
            file_paths: List of file paths to process
            anonymize: Whether to anonymize content
            restructure: Whether to restructure data
        
        Returns:
            List of processing results for each file
        """
        results = []
        
        logger.info(f"Starting batch processing: {len(file_paths)} files")
        
        for i, file_path in enumerate(file_paths, 1):
            logger.info(f"Processing file {i}/{len(file_paths)}: {os.path.basename(file_path)}")
            
            try:
                result = self.process_document(
                    file_path=file_path,
                    anonymize=anonymize,
                    restructure=restructure
                )
                results.append(result)
                
            except ProcessingError as e:
                # Continue with other files even if one fails
                error_result = {
                    'file_path': file_path,
                    'processing_timestamp': datetime.now().isoformat(),
                    'phases_completed': [],
                    'errors': [str(e)],
                    'warnings': [],
                    'data': None
                }
                results.append(error_result)
                logger.error(f"Failed to process {file_path}: {e}")
        
        logger.info(f"Batch processing completed: {len(results)} files processed")
        return results
    
    def save_processing_results(self, 
                              processing_result: Dict[str, Any], 
                              output_path: str = None) -> str:
        """
        Save processing results to a JSON file.
        
        Args:
            processing_result: Result from process_document
            output_path: Optional custom output path
        
        Returns:
            Path where results were saved
        """
        if output_path is None:
            # Generate output path based on input file
            input_file = processing_result['file_path']
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            output_dir = os.path.dirname(input_file) or '.'
            output_path = os.path.join(output_dir, f"{base_name}_processed.json")
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(processing_result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Processing results saved to: {output_path}")
            return output_path
            
        except Exception as e:
            error_msg = f"Failed to save processing results: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)
    
    def reset_stats(self):
        """Reset processing statistics."""
        self.processing_stats = {
            'files_processed': 0,
            'extraction_errors': 0,
            'anonymization_errors': 0,
            'restructuring_errors': 0,
            'last_processed': None
        }
        logger.info("Processing statistics reset")


# Convenience functions for quick processing
def process_single_file(file_path: str, 
                       password: str = None,
                       anonymize: bool = True, 
                       restructure: bool = True,
                       save_results: bool = True) -> Dict[str, Any]:
    """
    Convenience function to process a single file with default settings.
    
    Args:
        file_path: Path to the document
        password: Optional password for encrypted files
        anonymize: Whether to anonymize content
        restructure: Whether to restructure data
        save_results: Whether to save results to file
    
    Returns:
        Processing results dictionary
    """
    processor = DocumentProcessor()
    
    result = processor.process_document(
        file_path=file_path,
        password=password,
        anonymize=anonymize,
        restructure=restructure
    )
    
    if save_results:
        processor.save_processing_results(result)
    
    return result


def get_processing_summary(processing_result: Dict[str, Any]) -> str:
    """
    Generate a human-readable summary of processing results.
    
    Args:
        processing_result: Result from process_document
    
    Returns:
        Formatted summary string
    """
    file_name = os.path.basename(processing_result['file_path'])
    phases = processing_result['phases_completed']
    errors = processing_result['errors']
    warnings = processing_result['warnings']
    
    summary = f"Processing Summary for {file_name}:\n"
    summary += f"  Phases completed: {', '.join(phases)}\n"
    
    if errors:
        summary += f"  Errors: {len(errors)}\n"
        for error in errors:
            summary += f"    - {error}\n"
    
    if warnings:
        summary += f"  Warnings: {len(warnings)}\n"
        for warning in warnings:
            summary += f"    - {warning}\n"
    
    if not errors:
        summary += "  Status:Successfully completed\n"
    else:
        summary += "  Status:Completed with errors\n"
    
    return summary