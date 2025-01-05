import pandas as pd

df = pd.read_csv('/content/data.csv')

df.head()

df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

import pandas as pd

df = pd.read_csv('/content/data.csv')

df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

trips_distance_over_50 = df[df['trip_distance'] > 50]

print("Trips with distance over 50:")
print(trips_distance_over_50.head())

import pandas as pd

missing_payment_type = df[df['payment_type'].isnull()]

print("Trips with missing payment type:")
print(missing_payment_type.head().to_string(index=False))

import pandas as pd

trip_counts = df.groupby(['PULocationID', 'DOLocationID']).size().reset_index(name='trip_count')
print("Number of trips for each (PULocationID, DOLocationID) pair:")
print(trip_counts.head().to_string(index=False))

import pandas as pd
data_bad = df[df[['VendorID', 'passenger_count', 'store_and_fwd_flag', 'payment_type']].isnull().any(axis=1)]
data = df.drop(data_bad.index)
print("Rows with missing values (bad data):")
print(data_bad.head().to_string(index=False))

import pandas as pd
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
df['duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
print("Data with duration column:")
print(df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']].head().to_string(index=False))

pickup_trip_counts = data.groupby('PULocationID').size()
pickup_trip_counts = pickup_trip_counts.reset_index(name='trip_count')
print(pickup_trip_counts.head().to_string(index=False))

import pandas as pd
df['pickup_interval'] = df['tpep_pickup_datetime'].dt.floor('30T')

print("Data with pickup intervals:")
print(df[['tpep_pickup_datetime', 'pickup_interval']].head().to_string(index=False))

import pandas as pd
interval_stats = df.groupby('pickup_interval').agg(
    avg_passengers=('passenger_count', 'mean'),
    avg_fare_amount=('fare_amount', 'mean')
).reset_index()

print("Average passengers and fare amount for each interval:")
print(interval_stats.head().to_string(index=False))

import pandas as pd
interval_stats = df.groupby('pickup_interval').agg(
    avg_passengers=('passenger_count', 'mean'),
    avg_fare_amount=('fare_amount', 'mean')
).reset_index()
print("Average passengers and fare amount for each interval:")
print(interval_stats.head().to_string(index=False))

import pandas as pd
payment_interval_stats = df.groupby(['payment_type', 'pickup_interval']).agg(
    avg_fare_amount=('fare_amount', 'mean')
).reset_index()
max_fare_intervals = payment_interval_stats.loc[payment_interval_stats.groupby('payment_type')['avg_fare_amount'].idxmax()]
print("Interval with maximum average fare amount for each payment type:")
print(max_fare_intervals)

import pandas as pd
df['tip_to_fare_ratio'] = df['tip_amount'] / df['fare_amount']
df['tip_to_fare_ratio'] = df['tip_to_fare_ratio'].fillna(0)
tip_ratio_stats = df.dropna(subset=['payment_type']).groupby(['payment_type', 'pickup_interval']).agg(
    avg_tip_to_fare_ratio=('tip_to_fare_ratio', 'mean')
).reset_index()

max_tip_ratio_intervals = tip_ratio_stats.loc[tip_ratio_stats.groupby('payment_type', dropna=False)['avg_tip_to_fare_ratio'].idxmax()]

print("Interval with maximum average tip-to-fare ratio for each payment type:")
print(max_tip_ratio_intervals)

import pandas as pd
location_avg_fare = df.groupby('PULocationID').agg(avg_fare=('fare_amount', 'mean')).reset_index()
max_fare_location = location_avg_fare.loc[location_avg_fare['avg_fare'].idxmax()]
print("Location with the highest average fare amount:")
print(max_fare_location)

import pandas as pd

def top_5_destinations(group):
    """
    Finds the 5 most common destination locations for a given pickup location.

    Args:
        group: A pandas GroupBy object containing trips for a specific pickup location.

    Returns:
        A list of the 5 most common destination location IDs.
    """
    return group['DOLocationID'].value_counts().head(5).index.tolist()

common_destinations = df.groupby('PULocationID').apply(top_5_destinations)

common_destinations = common_destinations.explode().reset_index(name='DOLocationID')

common = data.merge(common_destinations, on=['PULocationID', 'DOLocationID'])

print("Common destinations dataframe:")
print(common.head())

import pandas as pd

common = data.merge(common_destinations, on=['PULocationID', 'DOLocationID'])
common['pickup_interval'] = df.loc[common.index, 'pickup_interval']

common_payment_interval_stats = common.groupby(['payment_type', 'pickup_interval']).agg(
    avg_fare_amount=('fare_amount', 'mean')
).reset_index()
print("Average fare amount on common destinations for each payment type and interval:")
print(common_payment_interval_stats.head().to_markdown(index=False, numalign="left", stralign="left"))

import pandas as pd
fare_diff = payment_interval_stats.merge(
    common_payment_interval_stats,
    on=['payment_type', 'pickup_interval'],
    suffixes=('_original', '_common')
)

fare_diff['fare_diff'] = fare_diff['avg_fare_amount_common'] - fare_diff['avg_fare_amount_original']

print("Differences in average fare amounts:")
print(fare_diff[['payment_type', 'pickup_interval', 'fare_diff']].head().to_markdown(index=False, numalign="left", stralign="left"))

import pandas as pd

df = pd.read_csv('data.csv.csv')
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

df = df.sort_values(by=['VendorID', 'tpep_pickup_datetime'])
df['chain'] = 0
chain_id = 0
prev_row = None

for idx, row in df.iterrows():
    row['tpep_pickup_datetime'] = pd.to_datetime(row['tpep_pickup_datetime'])
    if prev_row is not None:
        prev_row['tpep_dropoff_datetime'] = pd.to_datetime(prev_row['tpep_dropoff_datetime'])


        if (row['VendorID'] == prev_row['VendorID'] and
                row['PULocationID'] == prev_row['DOLocationID'] and
                (row['tpep_pickup_datetime'] - prev_row['tpep_dropoff_datetime']).total_seconds() <= 120):

            df.at[idx, 'chain'] = chain_id
        else:

            chain_id += 1
            df.at[idx, 'chain'] = chain_id
    prev_row = row


print("Data with trip chains:")
print(df[['VendorID', 'PULocationID', 'DOLocationID', 'chain']].head())
