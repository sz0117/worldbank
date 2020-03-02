#WRITE A GENERATOR TO LOAD DATA IN CHUNKS
print("(1)Process the first 1000 rows of a file line by line, \nto create a dictionary of the counts of \nhow many times each country appears in a column in the dataset:\n")
# Open a connection to the file
with open('/Users/sssssssidiiiiiiiiiii/Documents/DataCamp/WorldBank/.csv/Indicators.csv','rt') as file:
    # Skip the column names
    file.readline()

    # Initialize an empty dictionary: counts_dict
    counts_dict = {}

    # Process only the first 1000 rows
    for j in range(0,1000):

        # Split the current line into a list: line
        line = file.readline().split(',')

        # Get the value for the first column: first_col
        first_col = line[0]

        # If the column value is in the dict, increment its value
        if first_col in counts_dict.keys():
            counts_dict[first_col] += 1

        # Else, add to the dict and set value to 1
        else:
            counts_dict[first_col] = 1

# Print the resulting dictionary
print(counts_dict)

print("\n(2)lazily evaluate data:\ngenerate values in an efficient manner by yielding \nonly chunks of data at a time instead of the whole thing at once:\n")
# Define read_large_file()
def read_large_file(file_object):
    """A generator function to read a large file lazily."""

    # Loop indefinitely until the end of the file
    while True:

        # Read a line from the file: data
        data = file_object.readline()

        # Break if this is the end of the file
        if not data:
            break

        # Yield the line of data
        yield data

# Open a connection to the file
with open('/Users/sssssssidiiiiiiiiiii/Documents/DataCamp/WorldBank/.csv/Indicators.csv','rt') as file:

    # Create a generator object for the file: gen_file
    gen_file = read_large_file(file)

    # Print the first three lines of the file
    print(next(gen_file))
    print(next(gen_file))
    print(next(gen_file))

print("\n(3)Process the entire dataset line by line,\nto create a dictionary of the counts of \nhow many times each country appears in a column in the dataset:\n")
    # Initialize an empty dictionary: counts_dict
counts_dict = {}

# Open a connection to the file
with open('/Users/sssssssidiiiiiiiiiii/Documents/DataCamp/WorldBank/.csv/Indicators.csv','rt') as file:

    # Iterate over the generator from read_large_file()
    for line in read_large_file(file):

        row = line.split(',')
        first_col = row[0]

        if first_col in counts_dict.keys():
            counts_dict[first_col] += 1
        else:
            counts_dict[first_col] = 1

# Print
print(counts_dict)


print("\n(4)Another way to read data too large to store \nin memory in chunks is to read the file in \nas DataFrames of a certain length, say, 10")
# Import the pandas package
import pandas as pd

# Initialize reader object: df_reader
df_reader = pd.read_csv('/Users/sssssssidiiiiiiiiiii/Documents/DataCamp/WorldBank/.csv/Indicators.csv','rt', chunksize=10, engine='python')

# Print two chunks
print(next(df_reader))
print(next(df_reader))

print("\n(5)read a file using a bigger DataFrame chunk size \nand then process the data from the first chunk\n")
# Initialize reader object: urb_pop_reader
import pandas as pd
urb_pop_reader = pd.read_csv('/Users/sssssssidiiiiiiiiiii/Documents/DataCamp/WorldBank/.csv/Indicators.csv','rt', chunksize=1000, engine='python')

# Get the first DataFrame chunk: df_urb_pop
df_urb_pop = next(urb_pop_reader)

# Check out the head of the DataFrame
print(df_urb_pop.head())

# Check out specific country: df_pop_ceb
df_pop_ceb = df_urb_pop[df_urb_pop['CountryCode'] == 'CEB']

# Zip DataFrame columns of interest: pops
pops = zip(df_pop_ceb["Total Population"],
           df_pop_ceb["Urban population (% of total)"])

# Turn zip object into list: pops_list
pops_list = list(pops)

# Print pops_list
print(pops_list)


print("\n(6)cont. writing an iterator to load data in chunks\n")
import pandas as pd
# Code from previous exercise
urb_pop_reader = pd.read_csv('/Users/sssssssidiiiiiiiiiii/Documents/DataCamp/WorldBank/.csv/Indicators.csv', chunksize=1000)
df_urb_pop = next(urb_pop_reader)
df_pop_ceb = df_urb_pop[df_urb_pop['CountryCode'] == 'CEB']
pops = zip(df_pop_ceb['Total Population'],
           df_pop_ceb['Urban population (% of total)'])
pops_list = list(pops)

# Use list comprehension to create new DataFrame column 'Total Urban Population'
df_pop_ceb['Total Urban Population'] = [int(pops[0]*pops[1]*0.01) for pops in pops_list]

# Plot urban population data
df_pop_ceb.plot(kind='scatter', x='Year', y='Total Urban Population')
plt.show()

print("\n(7)cont. writing an iterator to load data in chunks\n")
import pandas as pd
# Initialize reader object: urb_pop_reader
urb_pop_reader = pd.read_csv('/Users/sssssssidiiiiiiiiiii/Documents/DataCamp/WorldBank/.csv/Indicators.csv', chunksize=1000)

# Initialize empty DataFrame: data
data = pd.DataFrame()

# Iterate over each DataFrame chunk
for df_urb_pop in urb_pop_reader:

    # Check out specific country: df_pop_ceb
    df_pop_ceb = df_urb_pop[df_urb_pop['CountryCode'] == 'CEB']

    # Zip DataFrame columns of interest: pops
    pops = zip(df_pop_ceb['Total Population'],
                df_pop_ceb['Urban population (% of total)'])

    # Turn zip object into list: pops_list
    pops_list = list(pops)

    # Use list comprehension to create new DataFrame column 'Total Urban Population'
    df_pop_ceb['Total Urban Population'] = [int(tup[0] * tup[1] * 0.01) for tup in pops_list]

    # Append DataFrame chunk to data: data
    data = data.append(df_pop_ceb)

# Plot urban population data
data.plot(kind='scatter', x='Year', y='Total Urban Population')
plt.show()

print("\n(8)cont. writing an iterator to load data in chunks\n")
import pandas as pd
# Define plot_pop()
def plot_pop(filename, country_code):

    # Initialize reader object: urb_pop_reader
    urb_pop_reader = pd.read_csv(filename, chunksize=1000)

    # Initialize empty DataFrame: data
    data = pd.DataFrame()

    # Iterate over each DataFrame chunk
    for df_urb_pop in urb_pop_reader:
        # Check out specific country: df_pop_ceb
        df_pop_ceb = df_urb_pop[df_urb_pop['CountryCode'] == country_code]

        # Zip DataFrame columns of interest: pops
        pops = zip(df_pop_ceb['Total Population'],
                    df_pop_ceb['Urban population (% of total)'])

        # Turn zip object into list: pops_list
        pops_list = list(pops)

        # Use list comprehension to create new DataFrame column 'Total Urban Population'
        df_pop_ceb['Total Urban Population'] = [int(tup[0] * tup[1] * 0.01) for tup in pops_list]

        # Append DataFrame chunk to data: data
        data = data.append(df_pop_ceb)

    # Plot urban population data
    data.plot(kind='scatter', x='Year', y='Total Urban Population')
    plt.show()

# Set the filename: fn
fn = 'ind_pop_data.csv'

# Call plot_pop for country code 'CEB'
plot_pop(fn,'CEB')

# Call plot_pop for country code 'ARB'
plot_pop(fn,'ARB')
