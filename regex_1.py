----test----
python "C:\Users\Asus\OneDrive\Documenten\Educom\Regex\regex_1.py" -p "Test" -f "C:\Users\Asus\OneDrive\Documenten\Educom\Regex\test_regex.pdf" -o "C:\Users\Asus\OneDrive\Documenten\Educom\Regex\output.html"

----script----
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
                        "page": page_num + 1,
                        "text": match.group(0),
                        "start": match.start(),
                        "end": match.end()
                    })
                    
        return matches

def generate_html(matches, output_file):
    # Create the HTML output
    html_content = "<html><body><h1>Search Results</h1><table border='1'><tr><th>Page</th><th>Match</th><th>Start</th><th>End</th></tr>"
    
    for match in matches:
        html_content += f"<tr><td>{match['page']}</td><td>{html.escape(match['text'])}</td><td>{match['start']}</td><td>{match['end']}</td></tr>"
    
    html_content += "</table></body></html>"
    
    # Write the HTML content to the output file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"HTML output written to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Search a PDF for a RegEx pattern and output results in HTML format")
    parser.add_argument('-p', '--pattern', required=True, help="The RegEx pattern to search for")
    parser.add_argument('-f', '--file', required=True, help="Path to the PDF file to search")
    parser.add_argument('-o', '--output', required=True, help="Output HTML file path")
    
    args = parser.parse_args()
    
    # Search the PDF for the pattern
    matches = search_pdf(args.pattern, args.file)
    
    # Generate the HTML output
    generate_html(matches, args.output)

if __name__ == "__main__":
    main()
