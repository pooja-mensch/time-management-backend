"""
Document Extractor Module
Handles extraction of text and tables from PDF and Excel files.
"""

import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from PyPDF2 import PdfReader 
import tabula
import pandas as pd
import msoffcrypto
from io import BytesIO
import pdfplumber

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Custom exception for extraction errors"""
    pass


class DocumentExtractor:
    """Handles extraction of content from various document formats."""
    
    SUPPORTED_FORMATS = {
        'pdf': ['.pdf'],
        'excel': ['.xlsx', '.xls']
    }
    
    def __init__(self):
        """Initialize the document extractor."""
        self.extraction_stats = {
            'pages_processed': 0,
            'tables_extracted': 0,
            'errors_encountered': []
        }
    
    def get_file_type(self, file_path: str) -> str:
        """
        Determine file type based on extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File type ('pdf', 'excel', or 'unsupported')
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        extension = os.path.splitext(file_path)[1].lower()
        
        for file_type, extensions in self.SUPPORTED_FORMATS.items():
            if extension in extensions:
                return file_type
        
        return 'unsupported'
    
    def extract_pdf_content(self, file_path: str, password: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract content from PDF file.
        
        Args:
            file_path: Path to PDF file
            password: Optional PDF password
            
        Returns:
            Dictionary containing extracted content
        """
        logger.info(f"Starting PDF extraction: {os.path.basename(file_path)}")
        
        results = {
            'file_type': 'pdf',
            'file_path': file_path,
            'pages': [],
            'metadata': {},
            'extraction_stats': {'pages_processed': 0, 'tables_found': 0, 'errors': []}
        }
        
        try:
            with open(file_path, "rb") as file:
                reader = PdfReader(file)
                
                # Handle encryption
                if reader.is_encrypted:
                    if not password:
                        raise ExtractionError("PDF is encrypted but no password provided")
                    
                    if not reader.decrypt(password):
                        raise ExtractionError("Invalid password for encrypted PDF")
                    
                    logger.info("Successfully decrypted PDF")
                
                # Extract metadata
                if reader.metadata:
                    results['metadata'] = {
                        'title': reader.metadata.get('/Title', ''),
                        'author': reader.metadata.get('/Author', ''),
                        'subject': reader.metadata.get('/Subject', ''),
                        'creator': reader.metadata.get('/Creator', ''),
                        'pages_count': len(reader.pages)
                    }
                
                # Process each page
                for page_num in range(1, len(reader.pages) + 1):
                    page_data = self._extract_pdf_page(file_path, page_num, password)
                    results['pages'].append(page_data)
                    results['extraction_stats']['pages_processed'] += 1
                    results['extraction_stats']['tables_found'] += len(page_data['tables'])
                
        except Exception as e:
            error_msg = f"PDF extraction failed: {str(e)}"
            logger.error(error_msg)
            results['extraction_stats']['errors'].append(error_msg)
            raise ExtractionError(error_msg)
        
        logger.info(f"PDF extraction completed: {results['extraction_stats']['pages_processed']} pages processed")
        return results
    
    def _extract_pdf_page(self, file_path: str, page_num: int, password: Optional[str] = None) -> Dict[str, Any]:
        """Extract content from a single PDF page."""
        page_data = {
            'page_number': page_num,
            'tables': [],
            'text_content': '',
            'errors': []
        }
        
        # Extract tables using tabula
        try:
            tables = tabula.read_pdf(
                file_path,
                pages=page_num,
                multiple_tables=True,
                password=password,
                silent=True
            )
            
            for i, table in enumerate(tables, start=1):
                if not table.empty:
                    table_dict = table.to_dict('records')
                    page_data['tables'].append({
                        'table_id': i,
                        'rows': len(table_dict),
                        'columns': len(table.columns),
                        'data': table_dict,
                        'column_names': table.columns.tolist()
                    })
            
        except Exception as e:
            error_msg = f"Table extraction failed on page {page_num}: {str(e)}"
            page_data['errors'].append(error_msg)
            logger.warning(error_msg)
        
        # Extract text using pdfplumber
        try:
            with pdfplumber.open(file_path, password=password) as pdf:
                page = pdf.pages[page_num - 1]
                
                # Get table bounding boxes to exclude table text
                table_bboxes = [t.bbox for t in page.find_tables()]
                words = page.extract_words()
                
                # Extract text outside tables
                page_text_words = []
                for word in words:
                    x0, y0, x1, y1 = word["x0"], word["top"], word["x1"], word["bottom"]
                    
                    # Check if word is inside any table
                    inside_table = any(
                        (x0 >= bx0 and x1 <= bx1 and y0 >= by0 and y1 <= by1)
                        for (bx0, by0, bx1, by1) in table_bboxes
                    )
                    
                    if not inside_table:
                        page_text_words.append(word["text"])
                
                page_data['text_content'] = " ".join(page_text_words).strip()
                
        except Exception as e:
            error_msg = f"Text extraction failed on page {page_num}: {str(e)}"
            page_data['errors'].append(error_msg)
            logger.warning(error_msg)
        
        return page_data
    
    def extract_excel_content(self, file_path: str, password: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract content from Excel file.
        
        Args:
            file_path: Path to Excel file
            password: Optional Excel password
            
        Returns:
            Dictionary containing extracted content
        """
        logger.info(f"Starting Excel extraction: {os.path.basename(file_path)}")
        
        results = {
            'file_type': 'excel',
            'file_path': file_path,
            'sheets': [],
            'metadata': {},
            'extraction_stats': {'sheets_processed': 0, 'rows_found': 0, 'errors': []}
        }
        
        try:
            with open(file_path, "rb") as file:
                excel_file = msoffcrypto.OfficeFile(file)
                
                # Handle encrypted files
                if excel_file.is_encrypted():
                    if not password:
                        raise ExtractionError("Excel file is encrypted but no password provided")
                    
                    try:
                        excel_file.load_key(password=password)
                        decrypted = BytesIO()
                        excel_file.decrypt(decrypted)
                        decrypted.seek(0)
                        df_dict = pd.read_excel(decrypted, sheet_name=None, header=None, engine="openpyxl")
                        logger.info("Successfully decrypted Excel file")
                    except Exception as e:
                        raise ExtractionError(f"Failed to decrypt Excel file: {str(e)}")
                else:
                    df_dict = pd.read_excel(file_path, sheet_name=None, header=None, engine="openpyxl")
            
            # Set metadata
            results['metadata'] = {
                'sheets_count': len(df_dict),
                'sheet_names': list(df_dict.keys())
            }
            
            # Process each sheet
            for sheet_name, dataframe in df_dict.items():
                sheet_data = self._extract_excel_sheet(sheet_name, dataframe)
                results['sheets'].append(sheet_data)
                results['extraction_stats']['sheets_processed'] += 1
                results['extraction_stats']['rows_found'] += sheet_data['non_empty_rows']
            
        except Exception as e:
            error_msg = f"Excel extraction failed: {str(e)}"
            logger.error(error_msg)
            results['extraction_stats']['errors'].append(error_msg)
            raise ExtractionError(error_msg)
        
        logger.info(f"Excel extraction completed: {results['extraction_stats']['sheets_processed']} sheets processed")
        return results
    
    def _extract_excel_sheet(self, sheet_name: str, dataframe: pd.DataFrame) -> Dict[str, Any]:
        """Extract content from a single Excel sheet."""
        sheet_data = {
            'sheet_name': sheet_name,
            'dimensions': {
                'rows': len(dataframe),
                'columns': len(dataframe.columns)
            },
            'content': [],
            'non_empty_rows': 0
        }
        
        if not dataframe.empty:
            # Clean empty rows
            clean_df = dataframe.dropna(how='all').reset_index(drop=True)
            
            for idx, row in clean_df.iterrows():
                # Extract non-empty cells
                row_cells = []
                for cell in row:
                    if pd.notna(cell) and str(cell).strip():
                        row_cells.append(str(cell).strip())
                
                if row_cells:
                    sheet_data['content'].append({
                        'row_index': idx,
                        'cells': row_cells,
                        'raw_text': ' | '.join(row_cells)
                    })
                    sheet_data['non_empty_rows'] += 1
        
        return sheet_data
    
    def extract(self, file_path: str, password: Optional[str] = None) -> Dict[str, Any]:
        """
        Main extraction method that handles different file types.
        
        Args:
            file_path: Path to the document
            password: Optional password for encrypted files
            
        Returns:
            Dictionary containing all extracted content
        """
        file_type = self.get_file_type(file_path)
        
        if file_type == 'pdf':
            return self.extract_pdf_content(file_path, password)
        elif file_type == 'excel':
            return self.extract_excel_content(file_path, password)
        else:
            raise ExtractionError(f"Unsupported file type: {file_type}")
    
    def get_extraction_summary(self, results: Dict[str, Any]) -> str:
        """Generate a human-readable summary of extraction results."""
        file_type = results.get('file_type', 'unknown')
        
        if file_type == 'pdf':
            pages = len(results.get('pages', []))
            tables = sum(len(page.get('tables', [])) for page in results.get('pages', []))
            return f"PDF processed: {pages} pages, {tables} tables extracted"
        
        elif file_type == 'excel':
            sheets = len(results.get('sheets', []))
            total_rows = sum(sheet.get('non_empty_rows', 0) for sheet in results.get('sheets', []))
            return f"Excel processed: {sheets} sheets, {total_rows} data rows extracted"
        
        return f"Unknown file type processed: {file_type}"