import boto3
import os
from dotenv import load_dotenv
import logging
import time
import re
from typing import List, Dict, Any
load_dotenv()

logging.basicConfig(filename='example.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROLE_ARN = os.getenv('ROLE_ARN')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')


sqs_resource = boto3.resource('sqs')

class Textract:
    # Class constants
    LABELS_STOP = {
        "Number and Street",
        "City",
        "County",
        "Zip Code",
        "Mailing Address Number and Street",
        "State",
    }

    def __init__(self, queries, bucket_name='cafo-contamination'):
        self.textract_client = boto3.client('textract', region_name=AWS_REGION_NAME)
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.queries_config = {
            "Queries": [
                {"Text": "Name of dairy or business operating the dairy", "Alias": "operator_name"},
                {"Text": "Physical address of dairy", "Alias": "physical_address"},
                {"Text": "City of dairy", "Alias": "city"},
                {"Text": "County", "Alias": "county"},
                {"Text": "Zip Code", "Alias": "zip"},
                {"Text": "Predominant milk cow breed", "Alias": "predominant_breed"},
                {"Text": "Average milk production", "Alias": "avg_milk_prod_lbs_per_cow_per_day"},
                {"Text": "Total nitrogen from manure", "Alias": "nitrogen_from_manure_lbs"},
                {"Text": "Process wastewater generated", "Alias": "process_wastewater_generated_gal"},
                {"Text": "Total phosphorus generated", "Alias": "phosphorus_generated_lbs"},
                {"Text": "Total potassium generated", "Alias": "potassium_generated_lbs"},
                {"Text": "Total salt generated", "Alias": "salt_generated_lbs"},
            ]
        }

    def test_textract(self):
        try:
            response = self.textract_client.list_adapters
            print("CONNECTED")
        except Exception as e:
            print(f"error: {e}")

    def check_file(self, test_file_key):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=test_file_key)
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except self.s3_client.exceptions.ClientError as e:
            print(f"Error accessing bucket: {e}")
            return False

    def start_job(self, s3_filename):
        try:
            response = self.textract_client.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': self.bucket_name,
                        'Name': s3_filename
                    }
                },
                FeatureTypes=["QUERIES"],
                QueriesConfig=self.queries_config,
                NotificationChannel={
                    "SNSTopicArn": SNS_TOPIC_ARN,
                    "RoleArn": ROLE_ARN
                }
            )

            return response['JobId']
        except Exception as e:
            print(f"Error detecting text: {e}")
            return None
    
    def get_job_status(self, job_id):
        try:
            response = self.textract_client.get_document_analysis(JobId=job_id)
            job_status = response['JobStatus']
            print(job_status)
            if job_status == 'SUCCEEDED':
                print("Response successful")
                return response
            else:
                print("Job not finished")
                return None
        except Exception as e:
            print(f"Error getting job: {e}")
            return None

    def wait_for_job_to_complete(self,job_id):
        while True:
            response = self.get_job_status(job_id)
            if response:
                print("Received result")
                return response['Blocks']
            print("Waiting for results")
            time.sleep(10)

    def clean(self, s: str) -> str:
        """Clean and normalize text by removing extra whitespace."""
        return re.sub(r"\s+", " ", s.strip())

    def as_num(self, s: str) -> float | None:
        """Convert string to float, handling commas and other formatting."""
        s = s.replace(",", "").strip()
        try:
            return float(s)
        except Exception:
            return None

    def extract_text(self, blocks):
        """Extract text from Textract blocks and return as a list of lines."""
        lines = []
        for item in blocks:
            if item['BlockType'] == "LINE":
                lines.append(item['Text'])
        return lines

    def analyze_all_blocks(self, blocks):
        """Analyze all blocks to understand the document structure and available data."""
        analysis = {
            'line_count': 0,
            'word_count': 0,
            'table_count': 0,
            'key_value_pairs': 0,
            'query_answers': 0,
            'all_text': []
        }
        
        for block in blocks:
            if block['BlockType'] == 'LINE':
                analysis['line_count'] += 1
                analysis['all_text'].append(block['Text'])
            elif block['BlockType'] == 'WORD':
                analysis['word_count'] += 1
            elif block['BlockType'] == 'TABLE':
                analysis['table_count'] += 1
            elif block['BlockType'] == 'KEY_VALUE_SET':
                analysis['key_value_pairs'] += 1
            elif block['BlockType'] == 'QUERY_RESULT':
                analysis['query_answers'] += 1
        
        return analysis

    def parse_general_info_from_lines(self, lines: List[str]) -> Dict[str, Any]:
        """Parse general information from a list of text lines extracted from a dairy report."""
        info: Dict[str, Any] = {
            "operator_name": None,
            "physical_address": {
                "street": None, "city": None, "county": None, "zip": None
            },
            "date_placed_in_operation": None,
            "basin_plan_designation": None,
            "assessor_parcels": [],
            "operator_phone": None,
            "mailing_address": {"street": None, "city": None, "state": None, "zip": None},
            "predominant_breed": None,
            "avg_milk_prod_lbs_per_cow_per_day": None,
            # Optional nutrient/process stats often useful "general" facts
            "nitrogen_from_manure_lbs": None,
            "phosphorus_generated_lbs": None,
            "potassium_generated_lbs": None,
            "salt_generated_lbs": None,
            "process_wastewater_generated_gal": None,
        }

        # Pre-compile key regexes
        re_operator = re.compile(r"^A\.\s*NAME OF DAIRY.*?:\s*(.+)$", re.I)
        re_date_started = re.compile(r"^Date facility was originally placed in operation:\s*(.+)$", re.I)
        re_basin = re.compile(r"^Regional Water Quality Control Board Basin Plan designation:\s*(.+)$", re.I)
        re_apn_label = re.compile(r"^County Assessor Parcel Number.*:", re.I)
        re_phone = re.compile(r"\(\d{3}\)\s*\d{3}-\d{4}")
        re_breed = re.compile(r"^Predominant milk\s*cow\s*breed:\s*(.+)$", re.I)
        re_avg_milk = re.compile(r"^Average milk production:\s*$", re.I)
        re_avg_milk_num = re.compile(r"(\d+(?:\.\d+)?)\s*pounds?\s*per\s*cow\s*per\s*day", re.I)

        re_total_n_from_manure = re.compile(r"^Total nitrogen from manure:\s*$", re.I)
        re_total_n_from_manure_value = re.compile(r"([\d,\.]+)\s*lbs\s*per\s*reporting\s*period", re.I)
        
        re_total_p_from_manure = re.compile(r"^Total phosphorus from manure:\s*$", re.I)
        re_total_p_from_manure_value = re.compile(r"([\d,\.]+)\s*lbs\s*per\s*reporting\s*period", re.I)
        
        re_total_k_from_manure = re.compile(r"^Total potassium from manure:\s*$", re.I)
        re_total_k_from_manure_value = re.compile(r"([\d,\.]+)\s*lbs\s*per\s*reporting\s*period", re.I)
        
        re_total_salt_from_manure = re.compile(r"^Total salt from manure:\s*$", re.I)
        re_total_salt_from_manure_value = re.compile(r"([\d,\.]+)\s*lbs\s*per\s*reporting\s*period", re.I)
        
        re_pwg = re.compile(r"^Process wastewater generated:\s*$", re.I)
        re_pwg_value = re.compile(r"([\d,\,\.]+)\s*gallons", re.I)

        # For address and mailing blocks
        want_physical_addr = False
        physical_addr_parts = []

        want_mailing_addr = False
        mailing_parts = []  # street, city, state, zip appear on subsequent lines

        i = 0
        n = len(lines)
        while i < n:
            raw = lines[i]
            line = self.clean(raw)

            # Operator name (same-line)
            m = re_operator.match(line)
            if m and not info["operator_name"]:
                info["operator_name"] = self.clean(m.group(1))
                i += 1
                continue

            # Physical address starts; next 3–4 lines are street, city, county, zip
            if re.match(r"^Physical address of dairy:?$", line, re.I):
                want_physical_addr = True
                physical_addr_parts = []
                i += 1
                continue

            if want_physical_addr:
                if not line or line in self.LABELS_STOP:
                    # Skip empty/label lines; keep scanning
                    i += 1
                    continue
                # Stop if we hit a new section header
                if ":" in line and not re.match(r"^\d", line):
                    # likely next labeled field; back up one step in spirit
                    want_physical_addr = False
                else:
                    physical_addr_parts.append(line)
                    # Heuristic: we need first 4 meaningful lines (street, city, county, zip)
                    if len(physical_addr_parts) >= 4:
                        want_physical_addr = False
                        street, city, county, zipc = physical_addr_parts[:4]
                        info["physical_address"] = {
                            "street": street,
                            "city": city,
                            "county": county,
                            "zip": zipc,
                        }
                    i += 1
                    continue

            # Date placed in operation
            m = re_date_started.match(line)
            if m and not info["date_placed_in_operation"]:
                info["date_placed_in_operation"] = self.clean(m.group(1))
                i += 1
                continue

            # Basin plan
            m = re_basin.match(line)
            if m and not info["basin_plan_designation"]:
                info["basin_plan_designation"] = self.clean(m.group(1))
                i += 1
                continue

            # APNs: label line then next line(s) of tokens until section changes
            if re_apn_label.match(line):
                # Collect APN tokens from subsequent lines until we hit a header-ish line
                j = i + 1
                apns = []
                while j < n:
                    cand = self.clean(lines[j])
                    if not cand or ":" in cand or cand.lower().startswith(("b.", "c.", "annual report", "available nutrients")):
                        break
                    # Tokenize space-separated APNs
                    apns.extend([t for t in cand.split() if "-" in t or "X" in t.upper()])
                    j += 1
                if apns:
                    info["assessor_parcels"] = apns
                i = j
                continue

            # Phone (first phone we see after operator block is fine)
            if not info["operator_phone"]:
                m = re_phone.search(line)
                if m:
                    info["operator_phone"] = m.group(0)

            # Mailing address block trigger (appears under "This operator is responsible..." section)
            if re.match(r"^P\.O\.\s*Box\b", line, re.I):
                want_mailing_addr = True
                mailing_parts = [line]  # first line is street (often "P.O. Box 1090")
                # Next lines: city, state, zip (likely each on separate lines)
                k = i + 1
                while k < n and len(mailing_parts) < 4:
                    cand = self.clean(lines[k])
                    if not cand or cand in self.LABELS_STOP:
                        k += 1
                        continue
                    # Stop if we hit a coloned label or a page footer
                    if ":" in cand or re.search(r"Page \d+ of \d+", cand, re.I):
                        break
                    mailing_parts.append(cand)
                    k += 1
                # Map to fields if we got 4 parts
                if len(mailing_parts) >= 4:
                    street, city, state, zipc = mailing_parts[:4]
                    info["mailing_address"] = {
                        "street": street,
                        "city": city,
                        "state": state,
                        "zip": zipc,
                    }
                i = k
                continue

            # Breed
            m = re_breed.match(line)
            if m and not info["predominant_breed"]:
                info["predominant_breed"] = self.clean(m.group(1))
                i += 1
                continue

            # Avg milk production (value on next line)
            m = re_avg_milk.match(line)
            if m and not info["avg_milk_prod_lbs_per_cow_per_day"]:
                # Look at next line for the value
                if i + 1 < n:
                    next_line = self.clean(lines[i + 1])
                    num_m = re_avg_milk_num.search(next_line)
                    if num_m:
                        info["avg_milk_prod_lbs_per_cow_per_day"] = self.as_num(num_m.group(1))
                i += 1
                continue

            # Nutrient stats (values on next lines)
            m = re_total_n_from_manure.match(line)
            if m and info["nitrogen_from_manure_lbs"] is None:
                # Look at next line for the value
                if i + 1 < n:
                    next_line = self.clean(lines[i + 1])
                    value_m = re_total_n_from_manure_value.search(next_line)
                    if value_m:
                        info["nitrogen_from_manure_lbs"] = self.as_num(value_m.group(1))
                i += 1
                continue

            m = re_total_p_from_manure.match(line)
            if m and info["phosphorus_generated_lbs"] is None:
                # Look at next line for the value
                if i + 1 < n:
                    next_line = self.clean(lines[i + 1])
                    value_m = re_total_p_from_manure_value.search(next_line)
                    if value_m:
                        info["phosphorus_generated_lbs"] = self.as_num(value_m.group(1))
                i += 1
                continue

            m = re_total_k_from_manure.match(line)
            if m and info["potassium_generated_lbs"] is None:
                # Look at next line for the value
                if i + 1 < n:
                    next_line = self.clean(lines[i + 1])
                    value_m = re_total_k_from_manure_value.search(next_line)
                    if value_m:
                        info["potassium_generated_lbs"] = self.as_num(value_m.group(1))
                i += 1
                continue

            m = re_total_salt_from_manure.match(line)
            if m and info["salt_generated_lbs"] is None:
                # Look at next line for the value
                if i + 1 < n:
                    next_line = self.clean(lines[i + 1])
                    value_m = re_total_salt_from_manure_value.search(next_line)
                    if value_m:
                        info["salt_generated_lbs"] = self.as_num(value_m.group(1))
                i += 1
                continue

            m = re_pwg.match(line)
            if m and info["process_wastewater_generated_gal"] is None:
                # Look at next line for the value
                if i + 1 < n:
                    next_line = self.clean(lines[i + 1])
                    value_m = re_pwg_value.search(next_line)
                    if value_m:
                        info["process_wastewater_generated_gal"] = self.as_num(value_m.group(1))
                i += 1
                continue

            i += 1

        # Post-fix: if city/county/zip were not captured in address loop (rare formatting), try single-line fallbacks
        if not info["physical_address"]["zip"]:
            for l in lines:
                lm = re.search(r"\b\d{5}(?:-\d{4})?\b", l)
                if lm:
                    info["physical_address"]["zip"] = lm.group(0)
                    break

        return info

    def parse_to_dataframe_row(self, lines: List[str]) -> Dict[str, Any]:
        """Parse text lines and return data in a flat format suitable for pandas DataFrame."""
        info = self.parse_general_info_from_lines(lines)
        
        # Flatten the nested dictionaries for DataFrame compatibility
        flat_info = {
            "operator_name": info["operator_name"],
            "physical_street": info["physical_address"]["street"],
            "physical_city": info["physical_address"]["city"],
            "physical_county": info["physical_address"]["county"],
            "physical_zip": info["physical_address"]["zip"],
            "date_placed_in_operation": info["date_placed_in_operation"],
            "basin_plan_designation": info["basin_plan_designation"],
            "assessor_parcels": ", ".join(info["assessor_parcels"]) if info["assessor_parcels"] else None,
            "operator_phone": info["operator_phone"],
            "mailing_street": info["mailing_address"]["street"],
            "mailing_city": info["mailing_address"]["city"],
            "mailing_state": info["mailing_address"]["state"],
            "mailing_zip": info["mailing_address"]["zip"],
            "predominant_breed": info["predominant_breed"],
            "avg_milk_prod_lbs_per_cow_per_day": info["avg_milk_prod_lbs_per_cow_per_day"],
            "nitrogen_from_manure_lbs": info["nitrogen_from_manure_lbs"],
            "phosphorus_generated_lbs": info["phosphorus_generated_lbs"],
            "potassium_generated_lbs": info["potassium_generated_lbs"],
            "salt_generated_lbs": info["salt_generated_lbs"],
            "process_wastewater_generated_gal": info["process_wastewater_generated_gal"],
        }
        
        return flat_info

    def extract_all_statistics(self, blocks) -> Dict[str, Any]:
        """Extract all statistical information from Textract blocks."""
        lines = self.extract_text(blocks)
        
        # Initialize comprehensive statistics dictionary
        stats = {
            # Basic facility info
            "facility_info": {
                "operator_name": None,
                "physical_address": {"street": None, "city": None, "county": None, "zip": None},
                "mailing_address": {"street": None, "city": None, "state": None, "zip": None},
                "operator_phone": None,
                "date_placed_in_operation": None,
                "basin_plan_designation": None,
                "assessor_parcels": [],
                "permit_number": None,
                "reporting_period": None,
            },
            
            # Herd information
            "herd_info": {
                "milk_cows": {"open_confinement": None, "under_roof": None, "max_number": None, "avg_number": None},
                "dry_cows": {"open_confinement": None, "under_roof": None, "max_number": None, "avg_number": None},
                "bred_heifers": {"open_confinement": None, "under_roof": None, "max_number": None, "avg_number": None},
                "heifers_7_14_mo": {"open_confinement": None, "under_roof": None, "max_number": None, "avg_number": None},
                "calves_4_6_mo": {"open_confinement": None, "under_roof": None, "max_number": None, "avg_number": None},
                "calves_0_3_mo": {"open_confinement": None, "under_roof": None, "max_number": None, "avg_number": None},
                "predominant_breed": None,
                "avg_milk_production_lbs_per_cow_per_day": None,
                "total_animals": None,
            },
            
            # Manure and nutrients
            "manure_nutrients": {
                "total_manure_excreted_tons": None,
                "total_manure_excreted_tons_per_period": None,
                "nitrogen_from_manure_lbs": None,
                "nitrogen_after_ammonia_losses_lbs": None,
                "phosphorus_from_manure_lbs": None,
                "potassium_from_manure_lbs": None,
                "salt_from_manure_lbs": None,
                "total_nitrogen_generated_lbs": None,
                "total_phosphorus_generated_lbs": None,
                "total_potassium_generated_lbs": None,
                "total_salt_generated_lbs": None,
            },
            
            # Water and wastewater
            "water_wastewater": {
                "process_wastewater_generated_gal": None,
                "process_wastewater_applied_gal": None,
                "fresh_water_imported_gal": None,
                "fresh_water_exported_gal": None,
                "total_water_balance_gal": None,
            },
            
            # Land application
            "land_application": {
                "total_land_applied_acres": None,
                "application_method": None,
                "application_timing": None,
                "soil_type": None,
            },
            
            # Compliance and monitoring
            "compliance": {
                "monitoring_wells": None,
                "groundwater_monitoring": None,
                "surface_water_discharge": None,
                "stormwater_management": None,
                "spill_prevention": None,
            }
        }
        
        # Parse the text using existing method for basic info
        basic_info = self.parse_general_info_from_lines(lines)
        
        # Transfer basic info to stats
        stats["facility_info"].update({
            "operator_name": basic_info["operator_name"],
            "physical_address": basic_info["physical_address"],
            "mailing_address": basic_info["mailing_address"],
            "operator_phone": basic_info["operator_phone"],
            "date_placed_in_operation": basic_info["date_placed_in_operation"],
            "basin_plan_designation": basic_info["basin_plan_designation"],
            "assessor_parcels": basic_info["assessor_parcels"],
        })
        
        # Transfer nutrient info
        stats["manure_nutrients"].update({
            "nitrogen_from_manure_lbs": basic_info["nitrogen_from_manure_lbs"],
            "phosphorus_from_manure_lbs": basic_info["phosphorus_generated_lbs"],
            "potassium_from_manure_lbs": basic_info["potassium_generated_lbs"],
            "salt_from_manure_lbs": basic_info["salt_generated_lbs"],
        })
        
        stats["water_wastewater"]["process_wastewater_generated_gal"] = basic_info["process_wastewater_generated_gal"]
        stats["herd_info"]["predominant_breed"] = basic_info["predominant_breed"]
        stats["herd_info"]["avg_milk_production_lbs_per_cow_per_day"] = basic_info["avg_milk_prod_lbs_per_cow_per_day"]
        
        # Enhanced parsing for additional statistics
        self._parse_herd_statistics(lines, stats)
        self._parse_manure_statistics(lines, stats)
        self._parse_water_statistics(lines, stats)
        self._parse_compliance_info(lines, stats)
        
        return stats

    def _parse_herd_statistics(self, lines: List[str], stats: Dict[str, Any]):
        """Parse detailed herd statistics from lines."""
        # Find the herd information section
        herd_start = None
        for i, line in enumerate(lines):
            if re.search(r'herd\s*information', line, re.I):
                herd_start = i
                break
        
        if herd_start is None:
            return
        
        # Define the animal types in order (as they appear in the document)
        animal_types = [
            'bred_heifers',      # Bred Heifers
            'heifers_7_14_mo',   # Heifers (7-14 mo)
            'calves_4_6_mo',     # Calves (4-6 mo)
            'calves_0_3_mo',     # Calves (0-3 mo)
            'milk_cows',         # Milk Cows
            'dry_cows'           # Dry Cows
        ]
        
        # Look for the data sections
        i = herd_start
        while i < len(lines) and i < herd_start + 50:  # Look within 50 lines of herd section
            line = self.clean(lines[i])
            
            # Find "Number open confinement" section
            if re.search(r'number\s*open\s*confinement', line, re.I):
                # The next 6 lines should be the open confinement numbers
                for j, animal_type in enumerate(animal_types):
                    if i + 1 + j < len(lines):
                        try:
                            value = int(lines[i + 1 + j].strip().replace(',', ''))
                            stats["herd_info"][animal_type]["open_confinement"] = value
                        except (ValueError, IndexError):
                            pass
            
            # Find "Number under roof" section
            elif re.search(r'number\s*under\s*roof', line, re.I):
                # The next 6 lines should be the under roof numbers
                for j, animal_type in enumerate(animal_types):
                    if i + 1 + j < len(lines):
                        try:
                            value = int(lines[i + 1 + j].strip().replace(',', ''))
                            stats["herd_info"][animal_type]["under_roof"] = value
                        except (ValueError, IndexError):
                            pass
            
            # Find "Maximum number" section
            elif re.search(r'maximum\s*number', line, re.I):
                # The next 6 lines should be the maximum numbers
                for j, animal_type in enumerate(animal_types):
                    if i + 1 + j < len(lines):
                        try:
                            value = int(lines[i + 1 + j].strip().replace(',', ''))
                            stats["herd_info"][animal_type]["max_number"] = value
                        except (ValueError, IndexError):
                            pass
            
            # Find "Average number" section
            elif re.search(r'average\s*number', line, re.I):
                # The next 6 lines should be the average numbers
                for j, animal_type in enumerate(animal_types):
                    if i + 1 + j < len(lines):
                        try:
                            value = int(lines[i + 1 + j].strip().replace(',', ''))
                            stats["herd_info"][animal_type]["avg_number"] = value
                        except (ValueError, IndexError):
                            pass
            
            i += 1
        
        # Calculate total animals from average numbers
        total_animals = 0
        for animal_type in animal_types:
            avg_number = stats["herd_info"][animal_type]["avg_number"]
            if avg_number is not None:
                total_animals += avg_number
        stats["herd_info"]["total_animals"] = total_animals if total_animals > 0 else None

    def _determine_count_type(self, lines: List[str], line_idx: int) -> str:
        """Determine what type of count a number represents."""
        # Look at context around the number to determine count type
        for i in range(max(0, line_idx-3), line_idx):
            line = self.clean(lines[i])
            if re.search(r'open\s*confinement', line, re.I):
                return 'open_confinement'
            elif re.search(r'under\s*roof', line, re.I):
                return 'under_roof'
            elif re.search(r'maximum\s*number', line, re.I):
                return 'max_number'
            elif re.search(r'average\s*number', line, re.I):
                return 'avg_number'
        return None

    def _parse_manure_statistics(self, lines: List[str], stats: Dict[str, Any]):
        """Parse detailed manure and nutrient statistics."""
        # Look for manure excretion data
        for i, line in enumerate(lines):
            line = self.clean(line)
            
            # Total manure excreted
            if re.search(r'total\s*manure\s*excreted', line, re.I):
                if i + 1 < len(lines):
                    next_line = self.clean(lines[i + 1])
                    # Look for tons per reporting period
                    tons_match = re.search(r'([\d,\.]+)\s*tons?\s*per\s*reporting\s*period', next_line, re.I)
                    if tons_match:
                        stats["manure_nutrients"]["total_manure_excreted_tons_per_period"] = self.as_num(tons_match.group(1))
            
            # Ammonia losses
            elif re.search(r'ammonia\s*losses', line, re.I):
                if i + 1 < len(lines):
                    next_line = self.clean(lines[i + 1])
                    nitrogen_match = re.search(r'([\d,\.]+)\s*lbs?\s*per\s*reporting\s*period', next_line, re.I)
                    if nitrogen_match:
                        stats["manure_nutrients"]["nitrogen_after_ammonia_losses_lbs"] = self.as_num(nitrogen_match.group(1))

    def _parse_water_statistics(self, lines: List[str], stats: Dict[str, Any]):
        """Parse water and wastewater statistics."""
        for i, line in enumerate(lines):
            line = self.clean(line)
            
            # Process wastewater applied
            if re.search(r'gallons?\s*applied', line, re.I):
                gallons_match = re.search(r'([\d,\.]+)\s*gallons?', line, re.I)
                if gallons_match:
                    stats["water_wastewater"]["process_wastewater_applied_gal"] = self.as_num(gallons_match.group(1))
            
            # Fresh water imported/exported
            elif re.search(r'gallons?\s*imported', line, re.I):
                gallons_match = re.search(r'([\d,\.]+)\s*gallons?', line, re.I)
                if gallons_match:
                    stats["water_wastewater"]["fresh_water_imported_gal"] = self.as_num(gallons_match.group(1))
            
            elif re.search(r'gallons?\s*exported', line, re.I):
                gallons_match = re.search(r'([\d,\.]+)\s*gallons?', line, re.I)
                if gallons_match:
                    stats["water_wastewater"]["fresh_water_exported_gal"] = self.as_num(gallons_match.group(1))

    def _parse_compliance_info(self, lines: List[str], stats: Dict[str, Any]):
        """Parse compliance and monitoring information."""
        for line in lines:
            line = self.clean(line)
            
            # Monitoring wells
            if re.search(r'monitoring\s*wells?', line, re.I):
                wells_match = re.search(r'(\d+)', line)
                if wells_match:
                    stats["compliance"]["monitoring_wells"] = int(wells_match.group(1))
            
            # Groundwater monitoring
            elif re.search(r'groundwater\s*monitoring', line, re.I):
                stats["compliance"]["groundwater_monitoring"] = True
            
            # Surface water discharge
            elif re.search(r'surface\s*water\s*discharge', line, re.I):
                stats["compliance"]["surface_water_discharge"] = True

    def extract_all_statistics_to_dataframe_row(self, blocks) -> Dict[str, Any]:
        """Extract all statistics and return in a flat format suitable for pandas DataFrame."""
        stats = self.extract_all_statistics(blocks)
        
        # Flatten the nested structure for DataFrame compatibility
        flat_stats = {}
        
        # Facility info
        flat_stats.update({
            'operator_name': stats['facility_info']['operator_name'],
            'physical_street': stats['facility_info']['physical_address']['street'],
            'physical_city': stats['facility_info']['physical_address']['city'],
            'physical_county': stats['facility_info']['physical_address']['county'],
            'physical_zip': stats['facility_info']['physical_address']['zip'],
            'mailing_street': stats['facility_info']['mailing_address']['street'],
            'mailing_city': stats['facility_info']['mailing_address']['city'],
            'mailing_state': stats['facility_info']['mailing_address']['state'],
            'mailing_zip': stats['facility_info']['mailing_address']['zip'],
            'operator_phone': stats['facility_info']['operator_phone'],
            'date_placed_in_operation': stats['facility_info']['date_placed_in_operation'],
            'basin_plan_designation': stats['facility_info']['basin_plan_designation'],
            'assessor_parcels': ', '.join(stats['facility_info']['assessor_parcels']) if stats['facility_info']['assessor_parcels'] else None,
            'permit_number': stats['facility_info']['permit_number'],
            'reporting_period': stats['facility_info']['reporting_period'],
        })
        
        # Herd info
        for animal_type, animal_data in stats['herd_info'].items():
            if isinstance(animal_data, dict):
                for count_type, count_value in animal_data.items():
                    flat_stats[f'{animal_type}_{count_type}'] = count_value
            else:
                flat_stats[animal_type] = animal_data
        
        # Manure and nutrients
        for key, value in stats['manure_nutrients'].items():
            flat_stats[key] = value
        
        # Water and wastewater
        for key, value in stats['water_wastewater'].items():
            flat_stats[key] = value
        
        # Land application
        for key, value in stats['land_application'].items():
            flat_stats[key] = value
        
        # Compliance
        for key, value in stats['compliance'].items():
            flat_stats[key] = value
        
        return flat_stats