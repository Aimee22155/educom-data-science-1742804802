python "C:\Users\Asus\OneDrive\Documenten\Educom\Regex\regex_2.py" 
-p "04-04-2025" 
-d "C:\Users\Asus\OneDrive\Documenten\Educom\Regex\test_regex_3.pdf" 
-o "C:\Users\Asus\OneDrive\Documenten\Educom\Regex\output_2.html"

----------

import os
import re
import PyPDF2
import argparse
import html

def search_pdf(patterns, pdf_path):
    # Open the PDF file
    with open(pdf_path, 'rb') as pdf_file:
        reader = PyPDF2.PdfReader(pdf_file)
        matches = []
        
        # Iterate through each page and search for the patterns
        for page_num, page in enumerate(reader.pages):
            text = page.extract_text()
            if text:
                # Search for each pattern
                for pattern, pattern_name in patterns:
                    for match in re.finditer(pattern, text):
                        matches.append({
                            "pdf": pdf_path,
                            "page": page_num + 1,
                            "pattern": pattern_name,
                            "text": match.group(0),
                            "start": match.start(),
                            "end": match.end()
                        })
                    
        return matches

def search_directory(patterns, directory):
    all_matches = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(".pdf"):
                pdf_path = os.path.join(root, file)
                matches = search_pdf(patterns, pdf_path)
                all_matches.extend(matches)
    return all_matches

def generate_html(matches, output_file):
    html_content = "<html><body><h1>Search Results</h1><table border='1'><tr><th>PDF</th><th>Page</th><th>Pattern</th><th>Match</th><th>Start</th><th>End</th></tr>"
    
    for match in matches:
        html_content += f"<tr><td>{html.escape(match['pdf'])}</td><td>{match['page']}</td><td>{html.escape(match['pattern'])}</td><td>{html.escape(match['text'])}</td><td>{match['start']}</td><td>{match['end']}</td></tr>"
    
    html_content += "</table></body></html>"
    
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"HTML output written to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Search for multiple RegEx patterns in PDFs and output results in HTML")
    parser.add_argument('-p', '--patterns', required=True, nargs='+', help="List of patterns to search for, e.g. 'date' 'invoice number' 'total amount'")
    parser.add_argument('-d', '--directory', required=True, help="Path to the directory with PDF files")
    parser.add_argument('-o', '--output', required=True, help="Output HTML file path")
    
    args = parser.parse_args()
    
    patterns = [(pattern, pattern_name) for pattern, pattern_name in zip(args.patterns, ["Date", "Invoice Number", "Total Amount"])]
    
    matches = search_directory(patterns, args.directory)
    
    generate_html(matches, args.output)

if __name__ == "__main__":
    main()
