import re

with open('python/delta/connect/tests/test_deltatable.py', 'r') as f:
    content = f.read()

# Fix whitespace and long line issues in the test we added
lines = content.split('\n')
for i, line in enumerate(lines):
    lines[i] = line.rstrip()

content = '\n'.join(lines)

with open('python/delta/connect/tests/test_deltatable.py', 'w') as f:
    f.write(content)
