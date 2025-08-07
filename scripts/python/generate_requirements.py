import os
import re
import sys
import pkgutil
import importlib.util

def find_imports(directory):
    imports = set()
    import_pattern = re.compile(r'^\s*(import|from)\s+([a-zA-Z0-9_\.]+)')
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                with open(os.path.join(root, file), 'r') as f:
                    for line in f:
                        match = import_pattern.match(line)
                        if match:
                            imports.add(match.group(2))
    return imports

def get_standard_libraries():
    standard_libs = set(sys.builtin_module_names)
    standard_libs.update(
        name for _, name, _ in pkgutil.iter_modules()
        if importlib.util.find_spec(name) is not None
    )
    return standard_libs

def filter_standard_libraries(imports):
    standard_libs = get_standard_libraries()
    return imports - standard_libs

def generate_requirements_txt(libraries, filename='requirements.txt'):
    unique_libraries = set()
    with open(filename, 'w') as f:
        for lib in sorted(libraries):
            base_lib = lib.split('.')[0]
            if base_lib not in unique_libraries:
                f.write(base_lib + '\n')
                unique_libraries.add(base_lib)

if __name__ == "__main__":
    imports = find_imports('.')
    filtered_imports = filter_standard_libraries(imports)
    generate_requirements_txt(filtered_imports)
    print("Generated requirements.txt")
