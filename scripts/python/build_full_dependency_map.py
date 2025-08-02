import os
import re
import json

BASH_EXEC_PATTERN = re.compile(r'\b(?:bash\s+|\./)([^\s]+(?:\.sh|\.py|\b))')
PYTHON_EXEC_PATTERN = re.compile(r'\bpython3?\s+([^\s]+\.py)')
GENERIC_EXEC_PATTERN = re.compile(r'\b([a-zA-Z0-9_\-]+)\s+.*')  # generic executable pattern
FILE_REDIRECT_INPUT = re.compile(r'<\s*([^\s]+)')
FILE_REDIRECT_OUTPUT = re.compile(r'(?:>|>>)\s*([^\s]+)')
CAT_PATTERN = re.compile(r'\bcat\s+([^\s]+)')
OPEN_FILE_PATTERN = re.compile(r'open\s*\(\s*[\'"]([^\'"]+)[\'"]\s*,\s*[\'"]([rw])')

# List common shell commands to ignore as executables (built-ins)
IGNORE_CMDS = {
    'if', 'then', 'else', 'fi', 'for', 'while', 'do', 'done', 'echo', 'exit', 'ls',
    'grep', 'sed', 'awk', 'mv', 'cp', 'rm', 'mkdir', 'printf', 'test', 'sleep'
}

def parse_bash_file(filepath):
    dependencies, executables, inputs, outputs = set(), set(), set(), set()
    with open(filepath, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()

        # Skip comments
        if line.startswith('#'):
            continue

        # Explicit bash/python execution
        dependencies.update(BASH_EXEC_PATTERN.findall(line))
        dependencies.update(PYTHON_EXEC_PATTERN.findall(line))

        # File redirections (inputs/outputs)
        inputs.update(FILE_REDIRECT_INPUT.findall(line))
        outputs.update(FILE_REDIRECT_OUTPUT.findall(line))

        # Files read with cat
        inputs.update(CAT_PATTERN.findall(line))

        # General executables
        match = GENERIC_EXEC_PATTERN.match(line)
        if match:
            cmd = match.group(1)
            if cmd not in IGNORE_CMDS and not cmd.endswith(('.sh', '.py')):
                executables.add(cmd)

    return dependencies, executables, inputs, outputs

def parse_python_file(filepath):
    dependencies, executables, inputs, outputs = set(), set(), set(), set()
    with open(filepath, 'r') as f:
        content = f.read()
        lines = content.splitlines()

    for line in lines:
        line = line.strip()

        # subprocess commands
        subproc_matches = re.findall(r'subprocess\.(?:run|call|Popen)\(\s*\[([^\]]+)\]', line)
        for match in subproc_matches:
            cmds = re.findall(r'["\']([^"\']+)["\']', match)
            for cmd in cmds:
                if cmd.endswith(('.py', '.sh')):
                    dependencies.add(cmd)
                elif cmd.split()[0] not in IGNORE_CMDS:
                    executables.add(cmd.split()[0])

        # file open operations
        open_match = OPEN_FILE_PATTERN.search(line)
        if open_match:
            filename, mode = open_match.groups()
            if mode == 'r':
                inputs.add(filename)
            else:
                outputs.add(filename)

    return dependencies, executables, inputs, outputs

def build_dependency_map(directory):
    dep_map, exec_map, input_map, output_map = {}, {}, {}, {}

    for root, _, files in os.walk(directory):
        for file in files:
            filepath = os.path.join(root, file)
            rel_filepath = os.path.relpath(filepath, directory)

            if file.endswith('.sh'):
                deps, execs, ins, outs = parse_bash_file(filepath)
            elif file.endswith('.py'):
                deps, execs, ins, outs = parse_python_file(filepath)
            else:
                continue

            dep_map[rel_filepath] = sorted(deps)
            exec_map[rel_filepath] = sorted(execs)
            input_map[rel_filepath] = sorted(ins)
            output_map[rel_filepath] = sorted(outs)

    return dep_map, exec_map, input_map, output_map

if __name__ == "__main__":
    directory = '.'  # Set to your project's directory
    dep_map, exec_map, input_map, output_map = build_dependency_map(directory)

    with open('script_dependencies.json', 'w') as f:
        json.dump(dep_map, f, indent=4)

    with open('script_executables.json', 'w') as f:
        json.dump(exec_map, f, indent=4)

    with open('script_input_files.json', 'w') as f:
        json.dump(input_map, f, indent=4)

    with open('script_output_files.json', 'w') as f:
        json.dump(output_map, f, indent=4)

    print("Generated:")
    print(" - script_dependencies.json")
    print(" - script_executables.json")
    print(" - script_input_files.json")
    print(" - script_output_files.json")
