"""
Brazil Oil & Gas Production Daily Scraper

A complete scraper that:
1. Uses Selenium to navigate and configure PowerBI dashboard
2. Makes API request to get data  
3. Processes JSON response and converts to CSV format

Project: brazil_oil_gas_production_daily - 17065
"""

import os
import time
import json
import sys
import csv
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def setup_chrome_driver():
    """Setup Chrome WebDriver with appropriate options for PowerBI scraping."""
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--disable-web-security')
    # chrome_options.add_argument('--headless')  # Uncomment for headless mode
    
    return webdriver.Chrome(options=chrome_options)


def navigate_powerbi_dashboard(historical: bool = False):
    """
    Step 1: Navigate PowerBI dashboard and configure filters.
    
    Args:
        historical: If True, scrape from 2000 to now; if False, last 14 days
        
    Returns:
        tuple: (start_date, end_date) in MM/dd/YYYY format
    """
    print("Step 1: Navigating PowerBI dashboard and configuring filters...")
    
    driver = setup_chrome_driver()
    
    try:
        wait = WebDriverWait(driver, 40)
        url = "https://app.powerbi.com/view?r=eyJrIjoiZjQ0NjIzNmYtNzY3Ni00MzZkLWI0MTQtYzk4ZWY0ZGI4ODQ5IiwidCI6IjQ0OTlmNGZmLTI0YTYtNGI0Mi1iN2VmLTEyNGFmY2FkYzkxMyJ9"
        driver.get(url)
        time.sleep(8)

        # Click the 'Instalações' imageBackground
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "imageBackground")))
        for element in driver.find_elements(By.CLASS_NAME, "imageBackground"):
            style = element.get_attribute("style")
            if "width: 190.229px" in style and "height: 40.9429px" in style:
                element.click()
                break
        time.sleep(8)

        # Set date range
        if historical:
            start_date = "01/01/2000"
            end_date = datetime.now().strftime("%m/%d/%Y")
        else:
            start_date = (datetime.now() - timedelta(days=13)).strftime("%m/%d/%Y")
            end_date = datetime.now().strftime("%m/%d/%Y")

        # Configure date inputs
        try:
            end_date_input = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[contains(@aria-label, 'End date')]")
            ))
            end_date_input.clear()
            end_date_input.send_keys(end_date + "\n")
            
            try:
                start_date_input = driver.find_element(By.XPATH, "//input[contains(@aria-label, 'Start date')]")
                start_date_input.clear()
                start_date_input.send_keys(start_date + "\n")
            except Exception:
                pass
        except Exception:
            pass
        time.sleep(8)

        # Expand 'Produção por Instalação'
        try:
            table_header = wait.until(EC.element_to_be_clickable((By.XPATH, "//h3[text()='Produção por Instalação']")))
            driver.execute_script("arguments[0].click();", table_header)
        except Exception:
            pass
        time.sleep(5)

        # Click Focus mode
        try:
            focus_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//button[contains(@aria-label, "Focus mode")]')
            ))
            driver.execute_script("arguments[0].click();", focus_btn)
        except Exception:
            pass
        time.sleep(10)

        print(f"   Date range configured: {start_date} to {end_date}")
        return start_date, end_date

    finally:
        driver.quit()


def convert_date_format(date_str: str) -> str:
    """Convert date from MM/dd/YYYY to YYYY-MM-dd format for API"""
    try:
        from datetime import datetime
        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return date_str


def make_powerbi_api_request(start_date: str, end_date: str):
    """
    Step 2: Make API request to PowerBI to get the data.
    
    Args:
        start_date: Start date in MM/dd/YYYY format
        end_date: End date in MM/dd/YYYY format
        
    Returns:
        dict: JSON response from PowerBI API
    """
    print("Step 2: Making PowerBI API request...")
    
    # Convert dates to YYYY-MM-dd format for the API
    api_start_date = convert_date_format(start_date)
    api_end_date = convert_date_format(end_date)
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://app.powerbi.com',
        'Referer': 'https://app.powerbi.com/',
        'User-Agent': 'Mozilla/5.0',
        'X-PowerBI-ResourceKey': 'f446236f-7676-436d-b414-c98ef4db8849'
    }
    
    params = {'synchronous': 'true'}
    
    json_data = {
        'version': '1.0.0',
        'queries': [
            {
                'Query': {
                    'Commands': [
                        {
                            'SemanticQueryDataShapeCommand': {
                                'Query': {
                                    'Version': 2,
                                    'From': [
                                        {'Name': 'd', 'Entity': 'Datas', 'Type': 0},
                                        {'Name': 'v', 'Entity': 'v_instalacoes_final', 'Type': 0},
                                        {'Name': 'm', 'Entity': 'Medidas', 'Type': 0},
                                        {'Name': 'c', 'Entity': 'Correção', 'Type': 0},
                                    ],
                                    'Select': [
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'd'}}, 'Property': 'Data'}, 'Name': 'Datas.Data'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Instalação'}, 'Name': 'v_instalacoes_final.Instalação'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Operadora'}, 'Name': 'v_instalacoes_final.Operadora'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Tipo'}, 'Name': 'v_instalacoes_final.Tipo'},
                                        {'Measure': {'Expression': {'SourceRef': {'Source': 'm'}}, 'Property': 'Petróleo'}, 'Name': 'Medidas.Petroleo'},
                                        {'Measure': {'Expression': {'SourceRef': {'Source': 'm'}}, 'Property': 'Gás Mm3'}, 'Name': 'Medidas.Gás'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Estado'}, 'Name': 'v_instalacoes_final.Estado'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Cidade'}, 'Name': 'v_instalacoes_final.Cidade'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Atendimento Campo'}, 'Name': 'v_instalacoes_final.Atendimento Campo'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Campo'}, 'Name': 'v_instalacoes_final.Campo'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'IND_ATIVO'}, 'Name': 'v_instalacoes_final.IND_ATIVO'},
                                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'qtde_pms'}}, 'Function': 0}, 'Name': 'Sum(v_instalacoes_final.qtde_pms)'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Silga'}, 'Name': 'v_instalacoes_final.Silga'},
                                        {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Situação'}, 'Name': 'v_instalacoes_final.Situação'},
                                        {'Measure': {'Expression': {'SourceRef': {'Source': 'm'}}, 'Property': 'Petróleo Equivalente boe'}, 'Name': 'Medidas.Petroleo Equivalente boe'},
                                    ],
                                    'Where': [
                                        {
                                            'Condition': {
                                                'Not': {
                                                    'Expression': {
                                                        'Comparison': {
                                                            'ComparisonKind': 0,
                                                            'Left': {
                                                                'Measure': {
                                                                    'Expression': {'SourceRef': {'Source': 'm'}},
                                                                    'Property': 'Petróleo Equivalente boe',
                                                                },
                                                            },
                                                            'Right': {'Literal': {'Value': 'null'}},
                                                        },
                                                    },
                                                },
                                            },
                                            'Target': [
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'd'}}, 'Property': 'Data'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Instalação'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Operadora'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Tipo'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Estado'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Cidade'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Atendimento Campo'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Campo'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'IND_ATIVO'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Silga'}},
                                                {'Column': {'Expression': {'SourceRef': {'Source': 'v'}}, 'Property': 'Situação'}},
                                            ],
                                        },
                                        {
                                            'Condition': {
                                                'In': {
                                                    'Expressions': [
                                                        {
                                                            'Column': {
                                                                'Expression': {
                                                                    'SourceRef': {
                                                                        'Source': 'c',
                                                                    },
                                                                },
                                                                'Property': 'Unidade',
                                                            },
                                                        },
                                                    ],
                                                    'Values': [
                                                        [
                                                            {
                                                                'Literal': {
                                                                    'Value': "'bbl'",
                                                                },
                                                            },
                                                        ],
                                                    ],
                                                },
                                            },
                                        },
                                        {
                                            'Condition': {
                                                'And': {
                                                    'Left': {
                                                        'Comparison': {
                                                            'ComparisonKind': 2,
                                                            'Left': {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd',
                                                                        },
                                                                    },
                                                                    'Property': 'Data',
                                                                },
                                                            },
                                                            'Right': {
                                                                'Literal': {
                                                                    'Value': f"datetime'{api_start_date}T00:00:00'",
                                                                },
                                                            },
                                                        },
                                                    },
                                                    'Right': {
                                                        'Comparison': {
                                                            'ComparisonKind': 3,
                                                            'Left': {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd',
                                                                        },
                                                                    },
                                                                    'Property': 'Data',
                                                                },
                                                            },
                                                            'Right': {
                                                                'Literal': {
                                                                    'Value': f"datetime'{api_end_date}T00:00:00'",
                                                                },
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                    'OrderBy': [
                                        {
                                            'Direction': 2,
                                            'Expression': {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'd',
                                                        },
                                                    },
                                                    'Property': 'Data',
                                                },
                                            },
                                        },
                                    ],
                                },
                                'Binding': {
                                    'Primary': {
                                        'Groupings': [
                                            {
                                                'Projections': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                                                'Subtotal': 1,
                                            },
                                        ],
                                    },
                                    'DataReduction': {
                                        'DataVolume': 3,
                                        'Primary': {
                                            'Window': {
                                                'Count': 500,
                                            },
                                        },
                                    },
                                    'Version': 1,
                                },
                                'ExecutionMetricsKind': 1,
                            },
                        },
                    ]
                },
                'CacheKey': '',
                'QueryId': '',
                'ApplicationContext': {
                    'DatasetId': '5dd23708-9095-4e35-b585-d1039d481990',
                    'Sources': [
                        {'ReportId': '0f6fa041-4098-458c-a4ac-1603e4eebbd2', 'VisualId': '7b566eb945004bec1197'}
                    ]
                }
            }
        ],
        'cancelQueries': [],
        'modelId': 3418545
    }

    response = requests.post(
        'https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata',
        params=params,
        headers=headers,
        json=json_data
    )

    data = response.json()
    
    # Save debug response for troubleshooting
    with open("debug_response.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    if "error" in data:
        raise Exception(f"API Error: {data['error']}")

    print("   API request successful, response saved to debug_response.json")
    return data


def extract_data_from_json(json_data: Dict[str, Any]):
    """
    Extract data arrays and value dictionaries from the Power BI JSON structure.
    
    Args:
        json_data: Full JSON data structure
        
    Returns:
        tuple: (data_rows, value_dicts, timestamp, row_date_assignments)
    """
    results = json_data.get('results', [])
    if not results:
        raise ValueError("No results found in JSON data")
    
    result_data = results[0].get('result', {}).get('data', {})
    timestamp = result_data.get('timestamp', '')
    
    dsr = result_data.get('dsr', {})
    ds_list = dsr.get('DS', [])
    if not ds_list:
        raise ValueError("No DS data found in JSON structure - PowerBI session may not be established")
    
    # Get ValueDicts from DS level
    value_dicts = ds_list[0].get('ValueDicts', {}) if ds_list else {}
    
    # Navigate to data rows
    ph_list = ds_list[0].get('PH', [])
    data_rows = []
    
    for ph in ph_list:
        # Get DM0 (summary rows)
        if 'DM0' in ph:
            for row in ph['DM0']:
                if 'C' in row:
                    data_rows.append(row['C'])
        
        # Get DM1 (detail rows) - main data
        if 'DM1' in ph:
            for row in ph['DM1']:
                if 'C' in row:
                    data_rows.append(row['C'])
    
    # Build date assignment based on row position groups
    current_date = ''
    row_date_assignments = []
    
    for i, row_data in enumerate(data_rows):
        if len(row_data) == 1 and isinstance(row_data[0], (int, float)) and row_data[0] > 1000000000000:
            current_date = convert_timestamp_to_date(row_data[0])
        elif len(row_data) >= 4 and isinstance(row_data[0], (int, float)) and row_data[0] > 1000000000000:
            current_date = convert_timestamp_to_date(row_data[0])
        row_date_assignments.append(current_date)
    
    return data_rows, value_dicts, timestamp, row_date_assignments


def map_data_row_to_csv(data_row: List[Any], value_dicts: Dict[str, List[str]], timestamp: str, row_date: str) -> Dict[str, Union[str, int, float]]:
    """
    Map a single data row to CSV format with proper filtering and validation.
    """
    def get_value(index):
        if len(data_row) > index and data_row[index] is not None:
            return data_row[index]
        return None
    
    def lookup_text_direct(index, dict_key):
        if index is not None and dict_key in value_dicts:
            if isinstance(index, int) and 0 <= index < len(value_dicts[dict_key]):
                text_value = value_dicts[dict_key][index]
                return text_value if text_value and text_value.strip() else ''
        return ''
    
    def get_numeric(value):
        if value is None:
            return ''
        try:
            if isinstance(value, str):
                clean_value = value.replace(',', '')
                if '.' in clean_value or 'e' in clean_value.lower():
                    num = float(clean_value)
                else:
                    num = int(float(clean_value))
            elif isinstance(value, (int, float)):
                num = value
            else:
                return ''
            return num if num != 0 else ''
        except (ValueError, TypeError):
            return ''
    
    # Initialize CSV row
    csv_row = {
        'scrape_datetime': timestamp,
        'date': row_date,
        'installation': '',
        'oil_production': '',
        'gas_production': '',
        'oil_equivalents_boe': '',
        'operator': '',
        'type': '',
        'estado': '',
        'city': '',
        'atendimento_campo': '',
        'campo': '',
        'quantity': '',
        'code': '',
        'status': ''
    }
    
    row_length = len(data_row)
    
    # Skip empty or invalid rows
    if row_length == 0:
        return None
    
    # Handle single timestamp rows - SKIP THESE
    if row_length == 1:
        return None
    
    # Handle summary rows (totals from DM0) - KEEP THESE
    if row_length == 4 and all(isinstance(val, (int, float, str)) and not isinstance(val, int) or val > 100 for val in data_row):
        csv_row['oil_production'] = get_numeric(data_row[0])
        csv_row['gas_production'] = get_numeric(data_row[1])
        csv_row['quantity'] = get_numeric(data_row[2])
        csv_row['oil_equivalents_boe'] = get_numeric(data_row[3])
        return csv_row
    
    # Handle timestamped summary rows
    first_val = get_value(0)
    if isinstance(first_val, (int, float)) and first_val > 1000000000000:
        if row_length == 4:
            csv_row['date'] = convert_timestamp_to_date(first_val)
            csv_row['oil_production'] = get_numeric(data_row[1])
            csv_row['gas_production'] = get_numeric(data_row[2])
            csv_row['oil_equivalents_boe'] = get_numeric(data_row[3])
            return csv_row
        else:
            return None  # Skip timestamp-only rows
    
    # Handle detail rows from DM1 - ONLY PROCESS COMPLETE ROWS
    if row_length >= 9:
        # Check if this row has valid installation data
        installation_idx = get_value(0)
        if not isinstance(installation_idx, int) or installation_idx < 0:
            return None  # Skip rows without valid installation index
        
        # Extract field mappings
        csv_row['installation'] = lookup_text_direct(get_value(0), 'D0')
        csv_row['operator'] = lookup_text_direct(get_value(1), 'D1')
        csv_row['type'] = lookup_text_direct(get_value(2), 'D2')
        csv_row['estado'] = lookup_text_direct(get_value(3), 'D3')
        csv_row['city'] = lookup_text_direct(get_value(4), 'D4')
        csv_row['atendimento_campo'] = lookup_text_direct(get_value(5), 'D5')
        csv_row['campo'] = lookup_text_direct(get_value(6), 'D6')
        
        # Skip rows without installation name
        if not csv_row['installation']:
            return None
        
        # Find production values - look for numeric values in the row
        production_values = []
        for i in range(6, row_length):  # Start after the field indices
            val = get_value(i)
            if isinstance(val, (int, float, str)):
                try:
                    num_val = float(str(val).replace(',', ''))
                    if num_val != 0:  # Only include non-zero values
                        production_values.append(num_val)
                except:
                    continue
        
        # Assign production values based on patterns
        if len(production_values) >= 3:
            # Look for quantity (typically a small integer 1-100)
            quantity_idx = -1
            for i, val in enumerate(production_values):
                if isinstance(val, (int, float)) and 1 <= val <= 1000 and val == int(val):
                    quantity_idx = i
                    csv_row['quantity'] = int(val)
                    break
            
            if quantity_idx >= 0:
                # Split production values around quantity
                before_qty = production_values[:quantity_idx]
                after_qty = production_values[quantity_idx + 1:]
                
                # Assign oil/gas based on what's available
                if len(before_qty) >= 2:
                    csv_row['oil_production'] = before_qty[0]
                    csv_row['gas_production'] = before_qty[1]
                elif len(before_qty) == 1:
                    # Determine based on installation type
                    if 'gás' in csv_row['type'].lower() or 'gas' in csv_row['type'].lower():
                        csv_row['gas_production'] = before_qty[0]
                    else:
                        csv_row['oil_production'] = before_qty[0]
                
                # BOE is typically the first value after quantity
                if len(after_qty) >= 1:
                    csv_row['oil_equivalents_boe'] = after_qty[0]
            else:
                # No clear quantity, assign first 3 values as oil, gas, boe
                if len(production_values) >= 3:
                    csv_row['oil_production'] = production_values[0]
                    csv_row['gas_production'] = production_values[1]
                    csv_row['oil_equivalents_boe'] = production_values[2]
        
        # Extract code and status from later positions
        for i in range(7, min(row_length, 11)):
            val = get_value(i)
            if isinstance(val, int) and 0 <= val < 1000:
                if i == 7:  # G8 - IND_ATIVO (skip)
                    continue
                elif i == 8:  # G9 - Code
                    csv_row['code'] = lookup_text_direct(val, 'D8')
                elif i == 9:  # G10 - Status
                    csv_row['status'] = lookup_text_direct(val, 'D9')
        
        # Only return rows that have meaningful data
        if csv_row['installation'] and (csv_row['oil_production'] or csv_row['gas_production'] or csv_row['oil_equivalents_boe']):
            return csv_row
    
    return None  # Skip all other row types


def process_json_to_csv(data_rows, value_dicts, timestamp, row_date_assignments, output_filename):
    """
    Process extracted data and write clean CSV with proper filtering.
    """
    csv_data = []
    
    for i, row_data in enumerate(data_rows):
        row_date = row_date_assignments[i] if i < len(row_date_assignments) else ''
        csv_row = map_data_row_to_csv(row_data, value_dicts, timestamp, row_date)
        
        # Only add valid rows
        if csv_row is not None:
            csv_data.append(csv_row)
    
    # Write to CSV
    write_csv_file(csv_data, output_filename)
    print(f"Processed {len(csv_data)} valid rows from {len(data_rows)} total rows")
    return csv_data

def convert_timestamp_to_date(timestamp_input) -> str:
    """
    Convert various timestamp formats to YYYY-MM-DD format.
    
    Args:
        timestamp_input: Unix timestamp (seconds or milliseconds) or date string
        
    Returns:
        Date string in YYYY-MM-DD format or empty string if invalid
    """
    if not timestamp_input:
        return ''
    
    try:
        if isinstance(timestamp_input, str):
            try:
                timestamp_num = float(timestamp_input)
            except ValueError:
                if len(timestamp_input) >= 10 and '-' in timestamp_input:
                    return timestamp_input[:10]
                return ''
        else:
            timestamp_num = float(timestamp_input)
        
        if timestamp_num <= 0:
            return ''
        
        if timestamp_num > 1e10:
            timestamp_sec = timestamp_num / 1000
        else:
            timestamp_sec = timestamp_num
        
        dt = datetime.fromtimestamp(timestamp_sec)
        return dt.strftime('%Y-%m-%d')
        
    except (ValueError, OSError, OverflowError):
        return ''



def write_csv_file(csv_data: List[Dict[str, Union[str, int, float]]], output_filename: str):
    """
    Write CSV data to file with exact column ordering.
    
    Args:
        csv_data: List of dictionaries with CSV row data
        output_filename: Output CSV filename
    """
    fieldnames = [
        'scrape_datetime', 'date', 'installation', 'oil_production', 
        'gas_production', 'oil_equivalents_boe', 'operator', 'type', 
        'estado', 'city', 'atendimento_campo', 'campo', 'quantity', 
        'code', 'status'
    ]
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in csv_data:
            clean_row = {}
            for field in fieldnames:
                value = row.get(field, '')
                # Convert all values to strings for CSV
                if value == '' or value is None:
                    clean_row[field] = ''
                else:
                    clean_row[field] = str(value)
            writer.writerow(clean_row)


def process_json_to_csv(json_data: Dict[str, Any], output_file: str):
    """
    Step 3: Process JSON response and convert to CSV format.
    
    Args:
        json_data: The JSON response from PowerBI API
        output_file: Output CSV filename
    """
    print("Step 3: Processing JSON response and creating CSV...")
    
    try:
        print("   Extracting data and dictionaries...")
        data_rows, value_dicts, timestamp, row_date_assignments = extract_data_from_json(json_data)
        print(f"   Found {len(data_rows)} data rows")
        print(f"   Date assignments: {len(row_date_assignments)} rows with dates")
        
        print("   Converting data rows to CSV format...")
        csv_data = []
        for i, data_row in enumerate(data_rows):
            try:
                row_date = row_date_assignments[i]
                csv_row = map_data_row_to_csv(data_row, value_dicts, timestamp, row_date)
                csv_data.append(csv_row)
            except Exception as e:
                print(f"   Warning: Error processing row {i}: {e}")
                continue
        
        print(f"   Writing {len(csv_data)} rows to {output_file}...")
        write_csv_file(csv_data, output_file)
        
        # Print summary statistics
        non_empty_rows = sum(1 for row in csv_data if any(str(row[field]).strip() for field in row if field != 'scrape_datetime'))
        print(f"   Summary: {non_empty_rows} rows with data out of {len(csv_data)} total rows")
        
    except Exception as e:
        print(f"   Error processing JSON to CSV: {e}")
        raise


def run(output_file: str, historical: bool = False):
    """
    Main entry point function for the scraper.
    Called by __main__.py with output filename.
    
    Args:
        output_file: Path to output CSV file
        historical: If True, scrape maximum historical data; if False, scrape last 14 days
    """
    print(f"Starting Brazil Oil & Gas Production scraper...")
    print(f"   Output file: {output_file}")
    print(f"   Historical mode: {historical}")
    
    try:
        # Step 1: Navigate and configure PowerBI dashboard
        start_date, end_date = navigate_powerbi_dashboard(historical)
        
        # Step 2: Make PowerBI API request
        json_data = make_powerbi_api_request(start_date, end_date)
        
        # Step 3: Process JSON and create CSV
        process_json_to_csv(json_data, output_file)
        
        print(f"Scraping completed successfully!")
        print(f"   Output saved to: {output_file}")
        print(f"   Debug data saved to: debug_response.json")
        
    except Exception as e:
        print(f"Scraping failed: {e}")
        sys.exit(1)