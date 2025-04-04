import os
import re
import PyPDF2
import argparse
import html

def search_pdf(pattern, pdf_path):
    # Open the PDF file
    with open(pdf_path, 'rb') as pdf_file:
        reader = PyPDF2.PdfReader(pdf_file)
        matches = []
        
        # Iterate through each page and search for the pattern
        for page_num, page in enumerate(reader.pages):
            text = page.extract_text()
            if text:
                # Find all occurrences of the pattern
                for match in re.finditer(pattern, text):
                    matches.append({
                        "pdf": pdf_path,
                        "page": page_num + 1,
                        "text": match.group(0),
                        "start": match.start(),
                        "end": match.end()
                    })
                    
        return matches

def search_directory(pattern, directory):
    all_matches = []
    # Walk through the directory and search each PDF file
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(".pdf"):
                pdf_path = os.path.join(root, file)
                matches = search_pdf(pattern, pdf_path)
                all_matches.extend(matches)
    return all_matches

def generate_html(matches, output_file):
    # Create the HTML output
    html_content = "<html><body><h1>Search Results</h1><table border='1'><tr><th>PDF</th><th>Page</th><th>Match</th><th>Start</th><th>End</th></tr>"
    
    for match in matches:
        html_content += f"<tr><td>{html.escape(match['pdf'])}</td><td>{match['page']}</td><td>{html.escape(match['text'])}</td><td>{match['start']}</td><td>{match['end']}</td></tr>"
    
    html_content += "</table></body></html>"
    
    # Write the HTML content to the output file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"HTML output written to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Search PDF files in a directory for a RegEx pattern and output results in HTML format")
    parser.add_argument('-p', '--pattern', required=True, help="The RegEx pattern to search for")
    parser.add_argument('-d', '--directory', required=True, help="Path to the directory with PDF files")
    parser.add_argument('-o', '--output', required=True, help="Output HTML file path")
    
    args = parser.parse_args()
    
    # Search all PDFs in the directory for the pattern
    matches = search_directory(args.pattern, args.directory)
    
    # Generate the HTML output
    generate_html(matches, args.output)

if __name__ == "__main__":
    main()
