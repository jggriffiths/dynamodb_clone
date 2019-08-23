import boto3


def do_clone(source_table, destination_table, delete_destination_data):

    if (delete_destination_data == 'yes please'):
        delete_destination_items(destination_table)
    else:
        print("Not deleting data.")

    copy_items(source_table, destination_table)


def delete_destination_items(destination_table):
    print("Deleting destination data")
    primary_key = get_primary_keys(destination_table)
    destination_scan = destination_table.scan()
    keys = get_primary_keys(destination_table)
    with destination_table.batch_writer() as batch:
        for i in destination_scan['Items']:
            delete_item(batch, keys, i)
        while 'LastEvaluatedKey' in destination_scan:
            destination_scan = destination_table.scan(ExclusiveStartKey=destination_scan['LastEvaluatedKey'])
            for i in destination_scan['Items']:
                delete_item(batch, keys, i)
    print("Done deleting destination data")


def delete_item(batch, keys, item):
    query = {}
    for key in keys:
        query.update({key['AttributeName']: item[key['AttributeName']]})
    print("Deleting " + item[keys[0]['AttributeName']])
    batch.delete_item(Key=query)


def copy_items(source_table, destination_table):
    source_scan = source_table.scan()
    primary_key = get_primary_keys(source_table)[0]['AttributeName']
    num_copied = 0
    with destination_table.batch_writer() as batch:
        for i in source_scan['Items']:
            copy(batch, i, primary_key)
            num_copied = num_copied + 1
            if num_copied % 1000 == 0:
                print ("Copied: " + str(num_copied))
        while 'LastEvaluatedKey' in source_scan:
            source_scan = source_table.scan(ExclusiveStartKey=source_scan['LastEvaluatedKey'])
            for i in source_scan['Items']:
                copy(batch, i, primary_key)
                num_copied = num_copied + 1
                if num_copied % 1000 == 0:
                    print ("Copied: " + str(num_copied))
    print ("Done.  Copied: " + str(num_copied))


def copy(batch, item, primary_key):
    obj_id = item[primary_key]
    print("Copying " + obj_id)
    batch.put_item(Item=item)


def get_primary_keys(table):
    print("Primary Key: " + str(table.key_schema[0]['AttributeName']))
    return table.key_schema



def main():
    print("We're about to copy the data from one DynamoDB table to another.  The destination table must already be cereated.")
    source_table_name = input("Source table name: ")
    source_account_key = input("Source account key: ")
    source_account_secret = input("Source account secret: ")
    source_account_region = input("Source account region: ")

    destination_table_name = input("Destination table name: ")
    destination_account_key = input("Destination account key: ")
    destination_account_secret = input("Destination account secret: ")
    destination_account_region = input("Destination account region: ")
        
    delete_data_first = input("Do you want to delete all items from the destination table? (only 'yes please' will be accepted) ")

    print("Source table: " + source_table_name)
    print("Destination table: " + destination_table_name)
    print("Deleting data: " + delete_data_first)
    

    source_table_dynamo = boto3.resource('dynamodb',    
                                                        aws_access_key_id=source_account_key,
                                                        aws_secret_access_key=source_account_secret,
                                                        region_name=source_account_region)

    source_table = source_table_dynamo.Table(source_table_name)

    destination_table_dynamo = boto3.resource('dynamodb',
                                                        aws_access_key_id=destination_account_key,
                                                        aws_secret_access_key=destination_account_secret,
                                                        region_name=destination_account_region)

    destination_table = destination_table_dynamo.Table(destination_table_name)


    do_clone(source_table, destination_table, delete_data_first)


if __name__ == "__main__":
    main()

