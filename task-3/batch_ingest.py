# Install necessary dependencies before running the code:
# sudo yum install python3-devel
# pip install happybase

# Import the happybase library
import happybase

# Create a connection to HBase
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

# Define the table name and column family
table_name = 'TaxiTrips'
column_family = 'TripDetails'

# Function to open the connection
def open_connection():
    connection.open()

# Function to close the opened connection
def close_connection():
    connection.close()

# Function to list all tables in HBase
def list_tables():
    print("Fetching all tables")
    open_connection()
    tables = connection.tables()
    decoded_tables = [table.decode('utf-8') for table in tables]
    close_connection()
    print("All tables fetched")
    return decoded_tables

# Function to create a table in HBase
def create_table(name, cf):
    print("Creating table " + name)
    tables = list_tables()
    if name not in tables:
        open_connection()
        connection.create_table(name, cf)
        close_connection()
        print("Table created")
    else:
        print("Table already present")

# Function to get the pointer to a table
def get_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table

# Function to batch insert data into the table
def batch_insert_data(filename, tablename):
    print("Starting batch insert of " + filename)
    file = open(filename, 'r')
    table = get_table(tablename)
    open_connection()
    i = 0
    with table.batch(batch_size=50000) as b:
        for line in file:
            if i != 0:
                # Parse the CSV line and construct a key
                temp = line.strip().split(",")
                key = temp[0] + '_' + temp[1] + '_' + temp[2]
                # Insert data into the HBase table
                b.put(key, {
                    'TripDetails:VendorID': str(temp[0]),
                    'TripDetails:tpep_pickup_datetime': str(temp[1]),
                    'TripDetails:tpep_dropoff_datetime': str(temp[2]),
                    'TripDetails:passenger_count': str(temp[3]),
                    'TripDetails:trip_distance': str(temp[4]),
                    'TripDetails:RatecodeID': str(temp[5]),
                    'TripDetails:store_and_fwd_flag': str(temp[6]),
                    'TripDetails:PULocationID': str(temp[7]),
                    'TripDetails:DOLocationID': str(temp[8]),
                    'TripDetails:payment_type': str(temp[9]),
                    'TripDetails:fare_amount': str(temp[10]),
                    'TripDetails:extra': str(temp[11]),
                    'TripDetails:mta_tax': str(temp[12]),
                    'TripDetails:tip_amount': str(temp[13]),
                    'TripDetails:tolls_amount': str(temp[14]),
                    'TripDetails:improvement_surcharge': str(temp[15]),
                    'TripDetails:total_amount': str(temp[16]),
                    'TripDetails:congestion_surcharge': str(temp[17]),
                    'TripDetails:airport_fee': str(temp[18])
                })
            i += 1

    file.close()
    print("Batch insert done")
    close_connection()

# Create the table if it doesn't exist
create_table(table_name, {column_family: dict(MAX_VERSIONS=1)})

# Batch insert data into the table from CSV files
batch_insert_data('yellow_tripdata_2017-03.csv', table_name)
batch_insert_data('yellow_tripdata_2017-04.csv', table_name)
