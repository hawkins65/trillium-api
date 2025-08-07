import os
import ast
from collections import defaultdict

def get_imports(directory):
    imports = defaultdict(list)
    for filename in os.listdir(directory):
        if filename.endswith('.py'):
            with open(filename, 'r', encoding='utf-8') as file:
                try:
                    tree = ast.parse(file.read(), filename=filename)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Import):
                            for alias in node.names:
                                imports[alias.name].append(filename)
                        elif isinstance(node, ast.ImportFrom):
                            for alias in node.names:
                                imports[alias.name].append(filename)
                except SyntaxError as e:
                    print(f"Syntax error in {filename}: {e}")
    return imports

if __name__ == '__main__':
    imports = get_imports('.')
    for module, files in imports.items():
        print(f"Module: {module}, Used in: {', '.join(files)}")