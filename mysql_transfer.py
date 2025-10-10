import mysql.connector
from mysql.connector import Error
import sys
from typing import Optional, Dict, List, Any
import json

class FlexibleMySQLTransfer:
    def __init__(self):
        self.source_conn = None
        self.dest_conn = None
    
    def connect_to_databases(self, source_config: dict, dest_config: dict) -> bool:
        """Connect to both source and destination databases"""
        try:
            # Connect to source database
            self.source_conn = mysql.connector.connect(**source_config)
            print(f"✓ Connected to source database: {source_config['host']}/{source_config['database']}")
            
            # Connect to destination database
            self.dest_conn = mysql.connector.connect(**dest_config)
            print(f"✓ Connected to destination database: {dest_config['host']}/{dest_config['database']}")
            
            return True
            
        except Error as e:
            print(f"✗ Database connection error: {e}")
            return False
    
    def transfer_with_mapping(self, transfer_config: dict) -> bool:
        """Transfer data with column mapping and transformations"""
        try:
            source_table = transfer_config['source_table']
            dest_table = transfer_config['dest_table']
            column_mapping = transfer_config['column_mapping']
            batch_size = transfer_config.get('batch_size', 1000)
            where_condition = transfer_config.get('where_condition', '')
            extra_columns = transfer_config.get('extra_columns', {})  # For destination-only columns
            
            print(f"📋 Starting transfer: {source_table} → {dest_table}")
            
            # Prepare cursors
            source_cursor = self.source_conn.cursor()
            dest_cursor = self.dest_conn.cursor()
            
            # Build source columns list (excluding skipped columns)
            source_columns = []
            dest_columns = []
            
            # Process main column mapping
            for src_col, dest_info in column_mapping.items():
                if isinstance(dest_info, dict) and dest_info.get('skip', False):
                    continue  # Skip this column
                source_columns.append(src_col)
                
                if isinstance(dest_info, str):
                    dest_columns.append(dest_info)
                elif isinstance(dest_info, dict):
                    if 'column' in dest_info:
                        dest_columns.append(dest_info['column'])
                    else:
                        dest_columns.append(src_col)
            
            # Add extra destination columns
            for extra_col, extra_info in extra_columns.items():
                dest_columns.append(extra_col)
            
            # Build SELECT query
            select_query = f"SELECT {', '.join(source_columns)} FROM {source_table}"
            if where_condition:
                select_query += f" WHERE {where_condition}"
            
            print(f"🔍 Query: {select_query}")
            
            # Count total records
            count_query = f"SELECT COUNT(*) FROM {source_table}"
            if where_condition:
                count_query += f" WHERE {where_condition}"
            
            source_cursor.execute(count_query)
            total_records = source_cursor.fetchone()[0]
            print(f"📊 Total records to transfer: {total_records}")
            
            if total_records == 0:
                print("⚠️ No records found to transfer")
                return True
            
            # Prepare INSERT query
            placeholders = ', '.join(['%s'] * len(dest_columns))
            insert_query = f"INSERT INTO {dest_table} ({', '.join(dest_columns)}) VALUES ({placeholders})"
            print(f"📝 Insert query: {insert_query}")
            
            # Transfer data in batches
            source_cursor.execute(select_query)
            transferred = 0
            
            while True:
                batch = source_cursor.fetchmany(batch_size)
                if not batch:
                    break
                
                # Transform batch data according to mapping
                transformed_batch = []
                for row in batch:
                    transformed_row = []
                    source_col_index = 0
                    
                    # Process main column mapping
                    for src_col, dest_info in column_mapping.items():
                        if isinstance(dest_info, dict) and dest_info.get('skip', False):
                            continue  # Skip this column completely
                        
                        source_value = row[source_col_index] if source_col_index < len(row) else None
                        source_col_index += 1
                        
                        if isinstance(dest_info, str):
                            # Simple mapping - use value as is
                            transformed_row.append(source_value)
                        elif isinstance(dest_info, dict):
                            # Complex mapping with transformations
                            if 'default_value' in dest_info:
                                # Use default value
                                transformed_row.append(dest_info['default_value'])
                            elif 'transform' in dest_info:
                                # Apply transformation function
                                transform_func = dest_info['transform']
                                transformed_value = transform_func(source_value)
                                transformed_row.append(transformed_value)
                            else:
                                # Use source value
                                transformed_row.append(source_value)
                    
                    # Add extra columns (destination-only)
                    for extra_col, extra_info in extra_columns.items():
                        if isinstance(extra_info, dict):
                            if 'default_value' in extra_info:
                                transformed_row.append(extra_info['default_value'])
                            elif 'transform' in extra_info:
                                transform_func = extra_info['transform']
                                transformed_row.append(transform_func())
                            else:
                                transformed_row.append(None)
                        else:
                            transformed_row.append(extra_info)  # Direct value
                    
                    transformed_batch.append(tuple(transformed_row))
                
                # Insert batch
                dest_cursor.executemany(insert_query, transformed_batch)
                self.dest_conn.commit()
                
                transferred += len(batch)
                progress = transferred / total_records * 100
                print(f"📦 Transferred {transferred}/{total_records} records ({progress:.1f}%)")
            
            source_cursor.close()
            dest_cursor.close()
            
            print(f"✅ Successfully transferred {transferred} records!")
            return True
            
        except Error as e:
            print(f"✗ Error during data transfer: {e}")
            if self.dest_conn:
                self.dest_conn.rollback()
            return False
    
    def close_connections(self):
        """Close database connections"""
        if self.source_conn and self.source_conn.is_connected():
            self.source_conn.close()
            print("✓ Source connection closed")
        
        if self.dest_conn and self.dest_conn.is_connected():
            self.dest_conn.close()
            print("✓ Destination connection closed")


# Transformation functions
def current_timestamp():
    """Return current timestamp"""
    from datetime import datetime
    return datetime.now()


def main():
    # Database configurations
    SOURCE_CONFIG = {
        'host': 'db21992.public.databaseasp.net',
        'port': 3306,
        'user': 'db21992',
        'password': 'oT@72-aEHr3#',
        'database': 'db21992'
    }
    
  
    DEST_CONFIG = {
        'host': 'db29413.public.databaseasp.net',
        'port': 3306,
        'user': 'db29413',
        'password': 's_3PM9c%=B8j',
        'database': 'db29413'
    }
    
    # ========================================
    # TRANSFER CONFIGURATION
    # ========================================
    
    # Example 1: Your specific use case  
    TRANSFER_CONFIG_1 = {
        'source_table': 'users',  # Source table name
        'dest_table': 'users',    # Destination table name
        'batch_size': 1000,
        'where_condition': '',    # Optional: 'id > 1000'
        'column_mapping': {
            # Source column → Destination mapping
            'id': 'id',                                    # Direct mapping
            'name': 'name',                                # Direct mapping
            'email': 'email',                              # Direct mapping
            'mobile': 'mobile',                            # Direct mapping
            'device_token': {'skip': True},                # Skip this column (not in destination)
            'password': 'hashing_password',                # Direct mapping - no hashing
            'created_at': 'created_at',                    # Direct mapping
        },
        'extra_columns': {
            # Columns that only exist in destination
            'created_by': {'default_value': 1},            # Set created_by to 1 for all records
            'modified_by': {'default_value': 1},           # Set modified_by to 1 for all records  
            'is_deleted': {'default_value': 0},            # Set is_deleted to 0 for all records
            'version': {'transform': current_timestamp},   # Set version to current timestamp
        }
    }
    
    # Example 2: Category table transfer
    TRANSFER_CONFIG_2 = {
        'source_table': 'categories',
        'dest_table': 'categories',
        'batch_size': 1000,
        'where_condition': '',    # Optional filter
        'column_mapping': {
            # Source column → Destination mapping
            'id': 'id',                                    # Direct mapping
            'en_title': 'name',                            # English title → name
            'ar_title': 'name_ar',                          # Arabic title → namear
            'pic': 'image',                            # pic → image_url
            'created_at': 'created_at',                    # Direct mapping
        },
        'extra_columns': {
            # Columns that only exist in destination
            'created_by': {'default_value': 1},            # Set created_by to 1
            'is_deleted': {'default_value': False},        # Set is_deleted to false
            'version': {'transform': current_timestamp},   # Set version to current timestamp
        }
    }
    
    # Example 3: Product table with JOIN to get user_id (handles duplicates)
    TRANSFER_CONFIG_3 = {
        'source_table': '''(
            SELECT p.id, p.category_id, up.user_id,p.en_url, p.created_at
            FROM product p 
            LEFT JOIN (
                SELECT product_id, user_id, 
                       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY id DESC) as rn
                FROM user_products
            ) up ON p.id = up.product_id AND up.rn = 1
        ) as source_query''',  # Subquery to handle duplicates
        'dest_table': 'products',
        'batch_size': 1000,
        'where_condition': '',    # Optional filter
        'column_mapping': {
            # Source column → Destination mapping
            'id': 'id',                                      # Product ID
            'category_id': 'category_id', 
             'en_url':'url',                                     # Category ID
            'user_id': 'user_id',                           # User ID (deduplicated)
            'created_at': 'created_at',                      # Created at
        },
        'extra_columns': {
            # Columns that only exist in destination
            'created_by': {'default_value': 1},              # Set created_by to 1
            'is_deleted': {'default_value': False},          # Set is_deleted to false
            'version': {'transform': current_timestamp},     # Set version to current timestamp
            'url_guid':{'default_value': None}
        }
    }
    
    # Example 3 Alternative: Simpler approach using GROUP BY
    TRANSFER_CONFIG_3_ALT = {
        'source_table': '''(
            SELECT p.id, p.category_id, 
                   (SELECT user_id FROM user_products up 
                    WHERE up.product_id = p.id 
                    ORDER BY up.id DESC LIMIT 1) as user_id,
                   p.created_at
            FROM product p
        ) as source_query''',
        'dest_table': 'products',
        'batch_size': 1000,
        'where_condition': '',
        'column_mapping': {
            'id': 'id',
            'category_id': 'category_id',
            'user_id': 'user_id',
            'created_at': 'created_at',
        },
        'extra_columns': {
            'created_by': {'default_value': 1},
            'is_deleted': {'default_value': False},
            'version': {'transform': current_timestamp},
        }
    }
    
    # Example 4: Different mapping scenario
    TRANSFER_CONFIG_4 = {
        'source_table': 'old_customers',
        'dest_table': 'new_customers',
        'batch_size': 500,
        'where_condition': 'status = "active"',
        'column_mapping': {
            'customer_id': 'id',
            'full_name': 'name',
            'email_address': 'email',
            'phone': 'mobile',
            'registration_date': 'created_at',
            'account_status': {
                'column': 'status',
                'transform': lambda x: 1 if x == 'active' else 0
            },
            'default_field': {
                'column': 'is_verified',
                'default_value': 0
            }
        }
    }
     # Example 5: Insert all users into user_roles table
    TRANSFER_CONFIG_5 = {
        'source_table': 'users',
        'dest_table': 'user_roles',
        'batch_size': 1000,
        'where_condition': '',    # Optional: add conditions like 'status = "active"'
        'column_mapping': {
            # Source column → Destination mapping
            'id': 'user_id',                                 # User ID from users table
        },
        'extra_columns': {
            # Columns that only exist in destination
            'role_id': {'default_value': 2},                 # Set role_id (change this number as needed)
            'is_deleted': {'default_value': 0},              # Set is_deleted to 0 (false)
            'version': {'transform': current_timestamp},     # Set version to current timestamp
            'created_by': {'default_value': 1},              # Set created_by to 1
            'created_at': {'transform': current_timestamp},  # Set created_at to current timestamp
            'modified_by': {'default_value': None},          # Set modified_by to NULL
            'modified_at': {'default_value': None},          # Set modified_at to NULL
            'deleted_by': {'default_value': None},           # Set deleted_by to NULL
            'deleted_at': {'default_value': None},           # Set deleted_at to NULL
        }
    }
    
    # ========================================
    # EXECUTION
    # ========================================
    
    # Choose which configuration to use
    # ACTIVE_CONFIG = TRANSFER_CONFIG_1  # For users table
    # ACTIVE_CONFIG = TRANSFER_CONFIG_2  # For category table
    ACTIVE_CONFIG = TRANSFER_CONFIG_3  # For product table with JOIN
    
    # To run multiple transfers, use this instead:
    # CONFIGS_TO_RUN = [TRANSFER_CONFIG_1, TRANSFER_CONFIG_2, TRANSFER_CONFIG_3]
    
    # Initialize transfer
    transfer = FlexibleMySQLTransfer()
    
    try:
        print("🚀 Starting flexible MySQL data transfer...")
        
        # Connect to databases
        if not transfer.connect_to_databases(SOURCE_CONFIG, DEST_CONFIG):
            sys.exit(1)
        
        # Option 1: Run single transfer
        if 'ACTIVE_CONFIG' in locals():
            print(f"📋 Single transfer: {ACTIVE_CONFIG['source_table']} → {ACTIVE_CONFIG['dest_table']}")
            success = transfer.transfer_with_mapping(ACTIVE_CONFIG)
            
            if success:
                print("🎉 Data transfer completed successfully!")
            else:
                print("❌ Data transfer failed!")
                sys.exit(1)
        
        # Option 2: Run multiple transfers (uncomment to use)
        # if 'CONFIGS_TO_RUN' in locals():
        #     print(f"📋 Running {len(CONFIGS_TO_RUN)} transfers...")
        #     for i, config in enumerate(CONFIGS_TO_RUN, 1):
        #         print(f"\n🔄 Transfer {i}/{len(CONFIGS_TO_RUN)}: {config['source_table']} → {config['dest_table']}")
        #         success = transfer.transfer_with_mapping(config)
        #         if not success:
        #             print(f"❌ Transfer {i} failed! Stopping.")
        #             sys.exit(1)
        #         print(f"✅ Transfer {i} completed!")
        #     
        #     print("🎉 All transfers completed successfully!")
            
    except KeyboardInterrupt:
        print("\n⚠️ Transfer interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        transfer.close_connections()


if __name__ == "__main__":
    main()
