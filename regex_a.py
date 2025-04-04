import re

text = "06 - 32345678"
pattern = re.compile(r'''
^                   # Start of string
(?:\s*)             # Optional leading spaces
(?:0|(?:\+31|0031)-?)  # National (0) or international (+31/0031) with optional dash
(?:\s*)             # Optional spaces
(?:
    6               # Mobile prefix
    |               # OR
    [1-9][0-9]{1,2} # 2 or 3 digit area code not starting with 0
)
(?:\s*-?\s*)        # Optional spaces and optional dash
[1-9][0-9]{6,7}     # Subscriber number (must not start with 0, makes total digits = 9)
$                   # End of string
''', re.VERBOSE)

# Use the re.search() function to find the first occurrence of the pattern in the text
match = re.search(pattern, text)

# Check if a match was found and print the result
if match:
    print("Match found:", match.group())
else:
    print("Match not found.")