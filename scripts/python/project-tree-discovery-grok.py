import os
import re
import json
import argparse
import ast
from typing import Dict, List, Optional

def parse_python_file(filepath: str) -> Dict:
    """Parse a Python file to extract objective, inputs, outputs, and called programs."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
        
        # Extract docstring for objective
        objective = ast.get_docstring(tree, clean=True) or "No objective specified."
        
        # Initialize sets to avoid duplicates
        inputs = set()
        outputs = set()
        called_programs = set()
        
        # Regular expressions for common input/output patterns
        input_patterns = {
            'csv': r'\b[\w\/\\-]+\.csv\b',
            'json': r'\b[\w\/\\-]+\.json\b',
            'params': r'argparse\.ArgumentParser|sys\.argv'
        }
        output_patterns = {
            'json': r'\b[\w\/\\-]+\.json\b',
            'png': r'\b[\w\/\\-]+\.png\b',
            'html': r'\b[\w\/\\-]+\.html\b',
            'csv': r'\b[\w\/\\-]+\.csv\b'
        }
        
        # Parse for inputs and outputs
        for pattern_type, pattern in input_patterns.items():
            if re.search(pattern, content):
                inputs.add(pattern_type)
        for pattern_type, pattern in output_patterns.items():
            if re.search(pattern, content):
                outputs.add(pattern_type)
        
        # Look for file operations (e.g., open, pandas.read_csv)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Check for file operations
                if isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr
                    if func_name in ('read_csv', 'read_json'):
                        inputs.add(func_name.split('_')[-1])
                    elif func_name in ('to_csv', 'to_json', 'savefig'):
                        outputs.add('csv' if func_name == 'to_csv' else 'json' if func_name == 'to_json' else 'png')
                
                # Check for subprocess or os.system calls
                if isinstance(node.func, ast.Name) and node.func.id in ('system', 'run'):
                    for arg in node.args:
                        if isinstance(arg, ast.Str):
                            # Extract command or script names
                            cmd = arg.s.strip()
                            if cmd.endswith(('.py', '.sh')):
                                called_programs.add(cmd)
                            elif cmd.split()[0] in ['python', 'bash']:
                                called_programs.add(cmd.split()[-1])
                            else:
                                called_programs.add(cmd.split()[0])  # Assume command-line tool
        
        return {
            'filename': os.path.basename(filepath),
            'objective': objective,
            'inputs': list(inputs),
            'outputs': list(outputs),
            'called_programs': list(called_programs)
        }
    except Exception as e:
        return {
            'filename': os.path.basename(filepath),
            'objective': f"Error parsing: {str(e)}",
            'inputs': [],
            'outputs': [],
            'called_programs': []
        }

def parse_bash_file(filepath: str) -> Dict:
    """Parse a Bash file to extract objective, inputs, outputs, and called programs."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract comments for objective
        comments = []
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('#'):
                comments.append(line[1:].strip())
        objective = ' '.join(comments) or "No objective specified."
        
        # Initialize sets to avoid duplicates
        inputs = set()
        outputs = set()
        called_programs = set()
        
        # Regular expressions for inputs/outputs
        input_patterns = {
            'csv': r'\b[\w\/\\-]+\.csv\b',
            'json': r'\b[\w\/\\-]+\.json\b',
            'params': r'\$1|\${1|\$@|\${@'
        }
        output_patterns = {
            'json': r'\b[\w\/\\-]+\.json\b',
            'png': r'\b[\w\/\\-]+\.png\b',
            'html': r'\b[\w\/\\-]+\.html\b',
            'csv': r'\b[\w\/\\-]+\.csv\b'
        }
        
        # Parse for inputs and outputs
        for pattern_type, pattern in input_patterns.items():
            if re.search(pattern, content):
                inputs.add(pattern_type)
        for pattern_type, pattern in output_patterns.items():
            if re.search(pattern, content):
                outputs.add(pattern_type)
        
        # Look for called programs
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split()
                if parts:
                    cmd = parts[0]
                    if cmd.endswith(('.py', '.sh')):
                        called_programs.add(cmd)
                    elif cmd in ['python', 'bash']:
                        if len(parts) > 1:
                            called_programs.add(parts[-1])
                    else:
                        called_programs.add(cmd)  # Assume command-line tool
        
        return {
            'filename': os.path.basename(filepath),
            'objective': objective,
            'inputs': list(inputs),
            'outputs': list(outputs),
            'called_programs': list(called_programs)
        }
    except Exception as e:
        return {
            'filename': os.path.basename(filepath),
            'objective': f"Error parsing: {str(e)}",
            'inputs': [],
            'outputs': [],
            'called_programs': []
        }

def build_file_tree(start_file: str, project_dir: str, visited: Optional[set] = None) -> Dict:
    """Recursively build a tree of program files starting from the given file."""
    if visited is None:
        visited = set()
    
    filepath = os.path.join(project_dir, start_file)
    if not os.path.exists(filepath) or start_file in visited:
        return {
            'filename': start_file,
            'objective': "File not found or already visited.",
            'inputs': [],
            'outputs': [],
            'called_programs': [],
            'dependencies': []
        }
    
    visited.add(start_file)
    
    # Parse based on file extension
    if start_file.endswith('.py'):
        node = parse_python_file(filepath)
    elif start_file.endswith('.sh'):
        node = parse_bash_file(filepath)
    else:
        # Handle command-line tools or unknown files
        node = {
            'filename': start_file,
            'objective': "Command-line tool or unsupported file type.",
            'inputs': [],
            'outputs': [],
            'called_programs': []
        }
    
    # Recursively process dependencies
    dependencies = []
    for prog in node['called_programs']:
        # Resolve relative paths
        prog_path = prog
        if not os.path.isabs(prog):
            prog_path = os.path.join(project_dir, prog)
        if os.path.exists(prog_path):
            dep_tree = build_file_tree(prog, project_dir, visited)
            dependencies.append(dep_tree)
    
    node['dependencies'] = dependencies
    return node

def main():
    parser = argparse.ArgumentParser(description="Build a tree of project files starting from a top-level file.")
    parser.add_argument('top_file', type=str, help="The top-level filename (e.g., script.sh or main.py)")
    parser.add_argument('--project-dir', type=str, default='.', help="The project directory (default: current directory)")
    
    args = parser.parse_args()
    
    # Build the file tree
    tree = build_file_tree(args.top_file, args.project_dir)
    
    # Save the tree as JSON
    output_file = 'project_file_tree.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(tree, f, indent=2)
    
    print(f"Project file tree has been saved to {output_file}")

if __name__ == "__main__":
    main()