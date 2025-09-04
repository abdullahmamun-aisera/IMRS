import os
import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime
import glob
from tqdm import tqdm
import time
from clickhouse_driver import Client as ClickHouseDriverClient

load_dotenv()

class WorkdayS3Extractor:
    def __init__(self):
        self.tenant_id = os.getenv("tenant_id", "workday_cx")
        # Use default AWS credentials from VM environment (IAM role/instance profile)
        self.session = boto3.Session()
        self.s3_client = self.session.client('s3')
        
        # Initialize ClickHouse connection
        self.clickhouse_client = None
        self.connect_clickhouse()
        
    def connect_clickhouse(self):
        """Connect to ClickHouse for getting storage_path"""
        try:
            self.clickhouse_client = ClickHouseDriverClient(
                host='central-clickhouse.aisera.cloud',
                port=9000,
                user=os.getenv("clickhouse_username"),
                password=os.getenv("clickhouse_password"),
                database='llm_analytics'
            )
            # Test connection
            self.clickhouse_client.execute("SELECT 1")
            print("[ClickHouse] Connected successfully")
            return True
        except Exception as e:
            print(f"[ClickHouse] Connection error: {e}")
            return False
    
    def get_storage_path_from_clickhouse(self, trace_id):
        """Get storage_path from ClickHouse using trace_id"""
        if not self.clickhouse_client:
            print(f"[WARN] ClickHouse not connected, cannot fetch storage_path for {trace_id}")
            return None
            
        try:
            query = """
            SELECT storage_path, tenant_id, prompt_name, created_at, namespace
            FROM llm_analytics.llm_analytics
            WHERE trace_id = %(trace_id)s
            LIMIT 1
            """
            
            result = self.clickhouse_client.execute(query, {'trace_id': trace_id})
            
            if result and len(result) > 0:
                row = result[0]
                storage_path = row[0] if row[0] else None
                tenant_id = row[1]
                prompt_name = row[2]
                created_at = row[3]
                namespace = row[4]
                
                print(f"[ClickHouse] Found data for trace_id {trace_id}")
                return {
                    'storage_path': storage_path,
                    'tenant_id': tenant_id,
                    'prompt_name': prompt_name,
                    'created_at': created_at,
                    'namespace': namespace
                }
            else:
                print(f"[WARN] No data found in ClickHouse for trace_id: {trace_id}")
                return None
                
        except Exception as e:
            print(f"[ERROR] Error querying ClickHouse for trace_id {trace_id}: {e}")
            return None
    
    def get_bucket_name_from_namespace(self, namespace):
        """Determine bucket name based on namespace"""
        return f"aiseratenants-{namespace}"
    
    def construct_s3_path(self, tenant_id, prompt_name, created_at, trace_id):
        """Construct S3 path when storage_path is null - following original logic"""
        try:
            if isinstance(created_at, str):
                dt = pd.to_datetime(created_at)
            else:
                dt = created_at
                
            year, month, day = dt.year, dt.month, dt.day
            return f"llm-analytics/{tenant_id}/{prompt_name}/{year}/{month}/{day}/{trace_id}.json"
        except Exception as e:
            print(f"[ERROR] Error constructing path for trace_id {trace_id}: {e}")
            return None
    
    def fetch_s3_content(self, bucket_name, s3_key, trace_id, max_retries=3):
        """Fetch content from S3 with retry logic - following original logic"""
        original_s3_key = s3_key
        
        for attempt in range(max_retries):
            try:
                # Direct object access
                response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                content = json.loads(response['Body'].read().decode('utf-8'))
                return content
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchKey':
                    print(f"[WARN] Key not found (attempt {attempt + 1}): {s3_key}")
                    if attempt == 0:
                        # Try next day - following original logic
                        parts = s3_key.split('/')
                        if len(parts) >= 7:
                            try:
                                day = int(parts[6]) + 1
                                parts[6] = str(day)
                                s3_key = '/'.join(parts)
                                print(f"[INFO] Trying next day: {s3_key}")
                                continue
                            except (ValueError, IndexError):
                                pass
                    
                    if attempt == max_retries - 1:
                        print(f"[ERROR] Key not found after all attempts: {original_s3_key}")
                        return {}
                        
                elif error_code == 'AccessDenied':
                    print(f"[ERROR] Access denied for key: {s3_key}")
                    return {}
                    
                else:
                    print(f"[ERROR] AWS ClientError: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                    else:
                        return {}
                        
            except Exception as e:
                print(f"[EXCEPTION] Unexpected error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    return {}
        
        return {}
    
    def extract_request_response_data(self, s3_content):
        """Extract request_params and response_message from S3 content"""
        extracted_data = {
            'request_params': None,
            'response_message': None,
            's3_fetch_success': False
        }
        
        if not s3_content:
            return extracted_data
            
        try:
            # Extract request parameters - look for exact key name 'request_params'
            if 'request_params' in s3_content:
                extracted_data['request_params'] = s3_content['request_params']
            
            # Extract response message - look for exact key name 'response_message'
            if 'response_message' in s3_content:
                extracted_data['response_message'] = s3_content['response_message']
            
            extracted_data['s3_fetch_success'] = True
            
        except Exception as e:
            print(f"[ERROR] Error extracting data from S3 content: {e}")
            
        return extracted_data
    
    def process_workday_files(self, input_dir="data/evaluated", output_dir="s3_enriched_data"):
        """Process all tenant_id files in the input directory structure"""
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Find all subdirectories in the input directory (prompt_name directories)
        if not os.path.exists(input_dir):
            print(f"[ERROR] Input directory not found: {input_dir}")
            return
        
        prompt_dirs = [d for d in os.listdir(input_dir) 
                      if os.path.isdir(os.path.join(input_dir, d))]
        
        if not prompt_dirs:
            print(f"[WARNING] No prompt directories found in: {input_dir}")
            return
        
        print(f"[INFO] Found {len(prompt_dirs)} prompt directories to scan")
        
        total_files_processed = 0
        
        for prompt_name in prompt_dirs:
            prompt_dir_path = os.path.join(input_dir, prompt_name)
            print(f"\n[INFO] Scanning prompt directory: {prompt_name}")
            
            # Find all CSV files starting with tenant_id (from env var)
            file_pattern = os.path.join(prompt_dir_path, f"{self.tenant_id}*.csv")
            files = glob.glob(file_pattern)
            
            if not files:
                print(f"[INFO] No files matching '{self.tenant_id}*.csv' found in {prompt_dir_path}")
                continue
            
            print(f"[INFO] Found {len(files)} files matching pattern in {prompt_name}")
            
            for file_path in files:
                print(f"\n[INFO] Processing file: {file_path}")
                self.process_single_file(file_path, output_dir, prompt_name)
                total_files_processed += 1
        
        print(f"\n[SUMMARY] Total files processed: {total_files_processed}")
    
    def process_single_file(self, file_path, output_dir, prompt_name=None):
        """Process a single CSV file"""
        try:
            # Read the input file
            df = pd.read_csv(file_path)
            print(f"[INFO] Loaded {len(df)} rows from {file_path}")
            
            # Initialize new columns for extracted data
            df['request_params'] = None
            df['response_message'] = None
            df['s3_fetch_success'] = False
            df['s3_key_used'] = None
            df['clickhouse_storage_path'] = None
            
            # Process each row
            for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
                try:
                    # Get trace_id from CSV
                    trace_id = row.get('trace_id')
                    
                    if pd.isna(trace_id):
                        print(f"[WARN] Row {index}: Missing trace_id, skipping")
                        continue
                    
                    # Step 1: Get storage_path and metadata from ClickHouse
                    ch_data = self.get_storage_path_from_clickhouse(trace_id)
                    
                    if ch_data:
                        storage_path = ch_data['storage_path']
                        tenant_id = ch_data['tenant_id']
                        prompt_name_ch = ch_data['prompt_name']
                        created_at = ch_data['created_at']
                        namespace = ch_data['namespace']
                        
                        # Store ClickHouse storage_path for debugging
                        df.at[index, 'clickhouse_storage_path'] = storage_path
                        
                    else:
                        # Fallback to CSV data if ClickHouse lookup fails
                        storage_path = row.get('storage_path')
                        tenant_id = row.get('tenant_id', self.tenant_id)
                        prompt_name_ch = row.get('prompt_name')
                        created_at = row.get('created_at')
                        namespace = row.get('namespace', 'prod1')
                        
                        print(f"[WARN] Row {index}: Using CSV data as fallback for trace_id {trace_id}")
                    
                    # Step 2: Determine S3 key
                    if storage_path and not pd.isna(storage_path) and storage_path.strip():
                        s3_key = storage_path
                        print(f"[INFO] Row {index}: Using storage_path from ClickHouse: {s3_key}")
                    else:
                        # Construct path using the original logic
                        s3_key = self.construct_s3_path(tenant_id, prompt_name_ch, created_at, trace_id)
                        if not s3_key:
                            print(f"[WARN] Row {index}: Could not construct S3 path")
                            continue
                        print(f"[INFO] Row {index}: Constructed S3 path: {s3_key}")
                    
                    # Step 3: Get bucket name
                    bucket_name = self.get_bucket_name_from_namespace(namespace)
                    
                    # Store the S3 key used for debugging
                    df.at[index, 's3_key_used'] = s3_key
                    
                    # Step 4: Fetch S3 content
                    s3_content = self.fetch_s3_content(bucket_name, s3_key, trace_id)
                    
                    # Step 5: Extract data
                    extracted_data = self.extract_request_response_data(s3_content)
                    
                    # Step 6: Update dataframe
                    df.at[index, 'request_params'] = json.dumps(extracted_data['request_params']) if extracted_data['request_params'] else None
                    df.at[index, 'response_message'] = json.dumps(extracted_data['response_message']) if extracted_data['response_message'] else None
                    df.at[index, 's3_fetch_success'] = extracted_data['s3_fetch_success']
                    
                    # Small delay to avoid overwhelming services
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"[ERROR] Error processing row {index}: {e}")
                    continue
            
            # Generate output filename
            input_filename = os.path.basename(file_path)
            # Include prompt_name in output filename if available
            if prompt_name:
                output_filename = f"{prompt_name}_enriched_{input_filename}"
            else:
                output_filename = f"enriched_{input_filename}"
            
            output_path = os.path.join(output_dir, output_filename)
            
            # Save enriched data
            df.to_csv(output_path, index=False)
            
            # Print summary
            success_count = df['s3_fetch_success'].sum()
            total_count = len(df)
            clickhouse_hits = df['clickhouse_storage_path'].notna().sum()
            
            print(f"\n[SUMMARY] File: {input_filename}")
            print(f"[SUMMARY] Total rows processed: {total_count}")
            print(f"[SUMMARY] ClickHouse hits: {clickhouse_hits}")
            print(f"[SUMMARY] Successful S3 fetches: {success_count}")
            print(f"[SUMMARY] Success rate: {success_count/total_count*100:.1f}%")
            print(f"[SUMMARY] Output saved to: {output_path}")
            
        except Exception as e:
            print(f"[ERROR] Error processing file {file_path}: {e}")

def main():
    """Main function to run the extractor"""
    print("=== Workday S3 Data Extractor with ClickHouse Integration ===")
    
    extractor = WorkdayS3Extractor()
    
    # Show configuration
    print(f"[CONFIG] Tenant ID: {extractor.tenant_id}")
    print(f"[CONFIG] Using VM default AWS credentials")
    print(f"[CONFIG] ClickHouse connected: {extractor.clickhouse_client is not None}")
    
    # Process all matching files
    extractor.process_workday_files(
        input_dir="data/evaluated",
        output_dir="s3_enriched_data"
    )
    
    print("\n=== Processing Complete ===")

if __name__ == "__main__":
    main()