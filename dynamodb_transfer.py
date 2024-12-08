import boto3
import sys

# AWS credentials and regions for source and target accounts
SOURCE_REGION = 'eu-north-1'
SOURCE_ACCESS_KEY = 'SOURCE_ACCESS_KEY'
SOURCE_SECRET_KEY = 'SOURCE_SECRET_KEY'

TARGET_REGION = 'eu-north-1'
TARGET_ACCESS_KEY = 'TARGET_ACCESS_KEY'
TARGET_SECRET_KEY = 'TARGET_SECRET_KEY'

# List of source-target table mappings
tables_to_transfer = [
    {"source": "Source_Table", "target": "Target_Table"},
]

# DynamoDB clients for source and target accounts
source_client = boto3.client(
    'dynamodb',
    region_name=SOURCE_REGION,
    aws_access_key_id=SOURCE_ACCESS_KEY,
    aws_secret_access_key=SOURCE_SECRET_KEY
)

target_client = boto3.client(
    'dynamodb',
    region_name=TARGET_REGION,
    aws_access_key_id=TARGET_ACCESS_KEY,
    aws_secret_access_key=TARGET_SECRET_KEY
)

# Transfer data between two tables
def transfer_table_data(source_table, target_table):
    print(f"Transferring data from {source_table} to {target_table}...")

    # Get the total number of items in the source table
    total_items_response = source_client.scan(
        TableName=source_table,
        Select='COUNT'
    )
    total_items = total_items_response.get('Count', 0)
    print(f"Total items to transfer: {total_items}")

    # Handle case when there are no items to transfer
    if total_items == 0:
        print("No items to transfer.")
        return

    # Paginate through all items in the source table
    paginator = source_client.get_paginator('scan')
    response_iterator = paginator.paginate(
        TableName=source_table,
        Select='ALL_ATTRIBUTES',
        ConsistentRead=True
    )

    # Transfer items and show progress
    transferred_count = 0
    for page in response_iterator:
        items = page.get('Items', [])
        for item in items:
            # Write each item to the target table
            target_client.put_item(
                TableName=target_table,
                Item=item
            )

            # Update progress
            transferred_count += 1
            progress_message = f"\rProgress: {transferred_count}/{total_items} items"
            sys.stdout.write(progress_message)
            sys.stdout.flush()

    
    print("\nData transfer completed.")


for table_mapping in tables_to_transfer:
    source_table = table_mapping['source']
    target_table = table_mapping['target']
    transfer_table_data(source_table, target_table)

print("All data transfers completed successfully.")
