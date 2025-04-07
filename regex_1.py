import re
import PyPDF2
import argparse
import os

# This function reads and extracts text from a PDF file
def read_pdf(file_path):
    try:
        with open(file_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            text = ''
            for page in reader.pages:
                text += page.extract_text() or ''  # Get text from each page
        return text
    except Exception as e:
        print(f"Error reading PDF file: {e}")
        exit(1)

# This function finds all matches of the regex pattern
def search_pattern(text, pattern):
    matches = re.findall(pattern, text)
    return matches

# This function saves matches to a simple HTML file
def save_to_html(matches, script_directory):
    # Specify the path for the HTML file
    output_path = os.path.join(script_directory, "results_1.html")  # Saves in the script directory
    try:
        with open(output_path, "w") as f:
            f.write("<html><body><h1>Matches Found</h1><ul>")
            for match in matches:
                f.write(f"<li>{match}</li>")
            f.write("</ul></body></html>")
        print(f"HTML file saved to: {output_path}")
    except Exception as e:
        print(f"Error saving HTML file: {e}")
        exit(1)

# Main program starts here
parser = argparse.ArgumentParser()
parser.add_argument('-p', help='Regex pattern to search', required=True)
parser.add_argument('-f', help='PDF file to scan', required=True)
args = parser.parse_args()

# Debugging - Ensure the argument is received properly
print(f"Pattern: {args.p}")
print(f"PDF File: {args.f}")

# Check if the file exists
if not os.path.exists(args.f):
    print(f"Error: The file {args.f} does not exist.")
    exit(1)

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Extract text from PDF and search for the pattern
pdf_text = read_pdf(args.f)
found = search_pattern(pdf_text, args.p)

# Save the matches to an HTML file
save_to_html(found, script_directory)

print("Done! Found", len(found), "matches.")
