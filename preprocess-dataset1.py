import pandas as pd
import csv


def preprocess_large_csv(input_file, output_file, delimiter='\t', chunk_size=10000):
    """
    Preprocess a large CSV file using Pandas, ensuring the first row is treated as the header
    and all subsequent rows align with the header. Saves the output to a new CSV file.

    :param input_file: Path to the large CSV file.
    :param output_file: Path to save the preprocessed CSV file.
    :param delimiter: Delimiter used in the CSV file (default is comma).
    :param chunk_size: Number of rows to process in each chunk.
    """
    try:
        # Read the header row (first line)
        with open(input_file, mode='r', encoding='utf-8') as file:
            header = file.readline().strip().split(delimiter)
        print(f"Header: {header}")

        # Open output file and write the header
        with open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=delimiter)
            writer.writerow(header)  # Write header to the output file

        # Process file in chunks
        for chunk_index, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size, skiprows=1,
                                                        header=None, delimiter=delimiter, dtype=str,
                                                        encoding='utf-8', on_bad_lines='skip')):
            # Iterate over each row in the chunk
            corrected_rows = []
            for row_number, row in chunk.iterrows():
                # Convert row to list
                row_data = list(row)

                # Fix row: align with the header
                if len(row_data) > len(header):
                    # Truncate extra columns
                    row_data = row_data[:len(header)]
                elif len(row_data) < len(header):
                    # Add missing columns as empty strings
                    row_data.extend([''] * (len(header) - len(row_data)))

                corrected_rows.append(row_data)

            # Append corrected rows to the output file
            with open(output_file, mode='a', encoding='utf-8', newline='') as outfile:
                writer = csv.writer(outfile, delimiter=delimiter)
                writer.writerows(corrected_rows)

        print("Preprocessing complete. Output saved to:", output_file)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def check_rows(file_path, row_indices, delimiter='\t'):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter)

        # Read the header
        header = next(reader)

        # Initialize a result dictionary for sampled rows
        sampled_rows = {}

        # Iterate through the file to fetch specific rows
        for current_index, row in enumerate(reader, start=1):  # Start counting from 1 (row 1 after header)
            if current_index in row_indices:
                sampled_rows[current_index] = row

            # Stop early if we've fetched all required rows
            if len(sampled_rows) == len(row_indices):
                    break

    # Display the header and sampled rows
    print("\nSampled Rows for Verification:")
    print("number of Header:", len(header))
    for idx in row_indices:
        if idx in sampled_rows:
            print(f"Row {idx}:", sampled_rows[idx])
            print(f'length of row: {len(sampled_rows[idx])}')
        else:
            print(f"Row {idx}: Not Found (out of range)")

# Define input and output file paths
input_file = "/Users/rui/Downloads/en.openfoodfacts.org.products.csv"  # Replace with your actual file path
output_file = "/Users/rui/Downloads/preprocessed_openfoodfacts.csv"  # Output file to save the preprocessed data

# Call the function to preprocess the CSV
preprocess_large_csv(input_file, output_file, delimiter='\t', chunk_size=10000) 

# check if the csv file is consistent, randomly sample a few rows from preprocessed file
check_rows(output_file, [35, 60, 110], delimiter='\t')
# number of header=206, number of values in one checked row = 206


