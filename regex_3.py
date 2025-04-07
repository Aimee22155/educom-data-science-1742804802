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
        return ""

# This function finds all matches of the regex pattern
def search_patterns(text, patterns):
    matches = {}
    for pattern_name, pattern in patterns.items():
        found = re.findall(pattern, text)
        matches[pattern_name] = found
    return matches

# This function saves matches to a simple HTML file
def save_to_html(matches, script_directory):
    output_path = os.path.join(script_directory, "results_3.html")
    try:
        with open(output_path, "w") as f:
            f.write("<html><body><h1>Matches Found</h1>")
            for pattern_name, found_matches in matches.items():
                f.write(f"<h2>{pattern_name}</h2><ul>")
                for match in found_matches:
                    f.write(f"<li>{match}</li>")
                f.write("</ul>")
            f.write("</body></html>")
        print(f"HTML file saved to: {output_path}")
    except Exception as e:
        print(f"Error saving HTML file: {e}")
        exit(1)

# Main program starts here
parser = argparse.ArgumentParser()
parser.add_argument('-d', help='PDF file to scan', required=True)
args = parser.parse_args()

# Debugging - Ensure the argument is received properly
print(f"PDF File: {args.d}")

# Define patterns for date, invoice number, and total amount
patterns = {
    'Invoice Number': r'INV-\d+',
    'Date (dd/mm/yyyy)': r'\d{2}/\d{2}/\d{4}',
    'Total Amount': r'\€\d+\.\d{2}'
}

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Extract text from the provided PDF
pdf_text = read_pdf(args.d)

# Search for all patterns in the PDF
matches = search_patterns(pdf_text, patterns)

# Save the matches to an HTML file
save_to_html(matches, script_directory)

print("Done! Found matches.")
