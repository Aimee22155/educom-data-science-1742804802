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
def search_pattern(text, pattern):
    matches = re.findall(pattern, text)
    return matches

# This function saves matches to a simple HTML file
def save_to_html(matches, script_directory):
    output_path = os.path.join(script_directory, "results_2.html")
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
parser.add_argument('-d', help='Directory to scan for PDF files', required=True)
args = parser.parse_args()

# Debugging - Ensure the argument is received properly
print(f"Pattern: {args.p}")
print(f"Directory: {args.d}")

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Loop through all PDF files in the provided directory
all_matches = []
for filename in os.listdir(args.d):
    if filename.endswith(".pdf"):
        pdf_file_path = os.path.join(args.d, filename)
        pdf_text = read_pdf(pdf_file_path)
        found = search_pattern(pdf_text, args.p)
        all_matches.extend(found)

# Save the matches to an HTML file
save_to_html(all_matches, script_directory)

print("Done! Found", len(all_matches), "matches.")
