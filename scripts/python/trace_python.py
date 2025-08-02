import os
import ast
import sys

def find_imports(file_path):
    imports = set()
    with open(file_path, 'r', encoding='utf-8') as f:
        tree = ast.parse(f.read(), filename=file_path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for n in node.names:
                    imports.add(n.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    return imports

def trace_directory(directory):
    dependency_map = {}
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                imports = find_imports(file_path)
                dependency_map[file_path] = list(imports)
    return dependency_map

if __name__ == "__main__":
    directory = sys.argv[1] if len(sys.argv) > 1 else '.'
    deps = trace_directory(directory)
    for file, imports in deps.items():
        print(f"{file}:")
        for imp in imports:
            print(f"    - {imp}")
