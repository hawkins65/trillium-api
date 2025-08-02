#!/usr/bin/env python3

import os
import re
import json
import ast
import argparse
from collections import defaultdict
import shlex # For more robust splitting of shell commands

# --- Helper Functions ---
def get_file_type_and_shebang_interpreter(filepath):
    """
    Determines the type of the file based on its extension or shebang.
    Returns a tuple: (file_type, shebang_interpreter)
    file_type can be: python, bash, cmd_tool, service, unknown_or_external
    shebang_interpreter is the interpreter specified in shebang (e.g., 'python3', 'bash'), or None.
    """
    if not os.path.exists(filepath) or not os.path.isfile(filepath):
        # Could be an external command or a directory (which we don't process as a "program")
        base = os.path.basename(filepath).lower()
        if base == "python" or base == "python3": return "python_interpreter", "python"
        if base == "bash" or base == "sh": return "bash_interpreter", "bash"
        return "unknown_or_external", None

    filename_lower = filepath.lower()
    if filename_lower.endswith((".sh", ".bash")):
        file_type = "bash"
    elif filename_lower.endswith(".py"):
        file_type = "python"
    elif filename_lower.endswith(".service"): # Basic support for systemd service files
        file_type = "service"
    else:
        file_type = "cmd_tool" # Default for other executable-looking files

    shebang_interpreter = None
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline().strip()
            if first_line.startswith("#!"):
                shebang_path = first_line[2:].strip()
                interpreter_cmd = os.path.basename(shebang_path)
                if "python" in interpreter_cmd:
                    shebang_interpreter = "python"
                    file_type = "python" # Override if extension was generic
                elif "bash" in interpreter_cmd or "sh" in interpreter_cmd:
                    shebang_interpreter = "bash"
                    file_type = "bash" # Override if extension was generic
                else:
                    shebang_interpreter = interpreter_cmd
    except Exception:
        pass # Ignore if cannot read

    return file_type, shebang_interpreter

def extract_objective_from_comments(filepath, file_type):
    """Tries to extract an objective from comments at the beginning of a file."""
    objective = "N/A"
    if not os.path.exists(filepath) or not os.path.isfile(filepath):
        return objective

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            comment_lines = []
            if file_type == "python":
                # Check for module-level docstring
                parsed_ast = None
                try:
                    # Read content again for AST parsing to avoid issues with partially read file
                    f.seek(0)
                    content = f.read()
                    parsed_ast = ast.parse(content)
                    docstring = ast.get_docstring(parsed_ast)
                    if docstring:
                        objective = " ".join(docstring.strip().splitlines())
                        return objective
                except SyntaxError: # If AST parsing fails, fall back to comment scraping
                    pass
                except Exception:
                    pass # other AST errors

                # Fallback to initial # comments if no docstring
                for line_idx, line_content in enumerate(lines):
                    stripped_line = line_content.strip()
                    if stripped_line.startswith("#"):
                        comment_lines.append(stripped_line[1:].strip())
                    # Allow for shebang or encoding declarations before comments
                    elif stripped_line and not stripped_line.startswith("#!") and not stripped_line.startswith("# -*- coding:"):
                        if comment_lines: break # Stop if comments were found and a non-comment line appears
                        elif line_idx > 5 : break # Don't look too far for initial comments
            elif file_type == "bash" or file_type == "service": # Treat service file comments like bash for now
                for line_idx, line_content in enumerate(lines):
                    stripped_line = line_content.strip()
                    if stripped_line.startswith("#") and not stripped_line.startswith("#!"):
                        comment_lines.append(stripped_line[1:].strip())
                    elif stripped_line and not stripped_line.startswith("#!"):
                         if comment_lines: break
                         elif line_idx > 5: break # Don't look too far

            if comment_lines:
                objective = " ".join(comment_lines)
    except Exception as e:
        print(f"Warning: Could not read comments from {filepath}: {e}")
    return objective if objective.strip() else "N/A"


# --- Python File Analysis ---
class PythonAnalyzer(ast.NodeVisitor):
    def __init__(self, script_path):
        self.inputs = set()
        self.outputs = set()
        self.calls = [] # list of dicts: {"cmd": executable, "args": list_of_args, "type": "sub_process"}
        self.script_dir = os.path.dirname(script_path)
        self.argparse_parsers = set() # Store names of ArgumentParser instances

    def _resolve_str_node(self, node):
        """Attempts to resolve an AST node to a string if it's a Constant (Py3.8+) or Str."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        elif isinstance(node, ast.Str): # For older Python versions
            return node.s
        elif isinstance(node, ast.JoinedStr): # f-string
            # Try to reconstruct f-string, might be lossy or contain unresolved variables
            parts = []
            for val_node in node.values:
                if isinstance(val_node, ast.Constant) and isinstance(val_node.value, str):
                    parts.append(val_node.value)
                elif isinstance(val_node, ast.FormattedValue):
                    # This is the {variable} part, hard to resolve statically here
                    # We'll just put a placeholder
                    if hasattr(val_node.value, 'id'): # simple variable
                         parts.append(f"{{variable:{val_node.value.id}}}")
                    else:
                         parts.append("{dynamic_fstring_part}")
                else: # ast.Str for older f-strings
                    parts.append(self._resolve_str_node(val_node) or "{unknown_fstring_part}")
            return "".join(parts)
        return None


    def visit_Assign(self, node):
        # Track ArgumentParser instances
        if isinstance(node.value, ast.Call) and \
           isinstance(node.value.func, ast.Attribute) and \
           isinstance(node.value.func.value, ast.Name) and \
           node.value.func.value.id == 'argparse' and \
           node.value.func.attr == 'ArgumentParser':
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.argparse_parsers.add(target.id)
        self.generic_visit(node)


    def visit_Call(self, node):
        # Check for open() calls
        if isinstance(node.func, ast.Name) and node.func.id == 'open':
            if len(node.args) > 0:
                filepath_str = self._resolve_str_node(node.args[0])
                mode = 'r' # default mode
                if len(node.args) > 1:
                    mode_str = self._resolve_str_node(node.args[1])
                    if mode_str: mode = mode_str
                elif len(node.keywords) > 0:
                    for kw in node.keywords:
                        if kw.arg == 'mode':
                            mode_str = self._resolve_str_node(kw.value)
                            if mode_str: mode = mode_str
                            break
                
                if filepath_str:
                    # Basic check for common extensions, could be expanded
                    if any(filepath_str.lower().endswith(ext) for ext in ['.csv', '.json', '.txt', '.xml', '.yaml', '.html', '.png', '.log']):
                        if 'r' in mode:
                            self.inputs.add(f"file_opened:{filepath_str}")
                        elif any(m in mode for m in ['w', 'a', 'x']):
                            self.outputs.add(f"file_opened:{filepath_str}")

        # Check for subprocess calls
        # e.g. subprocess.run, os.system
        func_name_parts = []
        current_func = node.func
        while isinstance(current_func, ast.Attribute):
            func_name_parts.insert(0, current_func.attr)
            current_func = current_func.value
        if isinstance(current_func, ast.Name):
            func_name_parts.insert(0, current_func.id)
        
        full_func_name = ".".join(func_name_parts)

        if full_func_name in ['subprocess.run', 'subprocess.call', 'subprocess.Popen', 'os.system']:
            if node.args:
                cmd_arg_node = node.args[0]
                command_parts = []

                if isinstance(cmd_arg_node, (ast.List, ast.Tuple)): # ['cmd', 'arg1']
                    for el in cmd_arg_node.elts:
                        part = self._resolve_str_node(el)
                        if part:
                            command_parts.append(part)
                        elif isinstance(el, ast.Name): # variable
                            command_parts.append(f"variable:{el.id}")
                        else:
                            command_parts.append("dynamic_arg")
                elif isinstance(cmd_arg_node, (ast.Constant, ast.Str)): # "cmd arg1" or f"cmd {var}"
                    cmd_string = self._resolve_str_node(cmd_arg_node)
                    if cmd_string:
                        # Use shlex to handle quoted arguments in the string
                        try:
                            command_parts = shlex.split(cmd_string)
                        except ValueError: # Handle unclosed quotes, etc.
                            command_parts = cmd_string.split() # Fallback
                    else:
                        command_parts = ["dynamic_command_string"]
                
                if command_parts:
                    self.calls.append({"cmd": command_parts[0], "args": command_parts[1:], "type": "sub_process"})

        # Check for argparse add_argument
        # e.g. parser.add_argument('-f', '--file')
        if full_func_name.endswith('.add_argument') and func_name_parts[0] in self.argparse_parsers :
            arg_names = []
            help_text = None
            action = None
            is_output_heuristic = False # Heuristic: if 'output' in name or help

            for arg in node.args: # e.g. '-f', '--foo'
                arg_str = self._resolve_str_node(arg)
                if arg_str:
                    arg_names.append(arg_str)
                    if 'output' in arg_str.lower() or 'out' in arg_str.lower():
                        is_output_heuristic = True
            
            for kw in node.keywords:
                if kw.arg == 'help':
                    help_text = self._resolve_str_node(kw.value)
                    if help_text and ('output' in help_text.lower() or 'write to' in help_text.lower()):
                        is_output_heuristic = True
                elif kw.arg == 'action':
                    action = self._resolve_str_node(kw.value)
                elif kw.arg == 'dest': # e.g. dest="output_file"
                    dest_val = self._resolve_str_node(kw.value)
                    if dest_val and ('output' in dest_val.lower() or 'out' in dest_val.lower()):
                        is_output_heuristic = True
            
            param_name = ", ".join(arg_names) if arg_names else "N/A"
            param_info = f"{param_name}"
            if help_text: param_info += f" (help: {help_text[:50]}{'...' if len(help_text) > 50 else ''})"

            # Determine if it's primarily input or output based on common actions or heuristics
            if action in ['store_true', 'store_false', 'count']:
                 self.inputs.add(f"param_flag:{param_info}")
            elif is_output_heuristic or (action and 'write' in action.lower()):
                 self.outputs.add(f"param_output_arg:{param_info}")
            else: # Default to input parameter
                 self.inputs.add(f"param_input_arg:{param_info}")

        self.generic_visit(node)

def analyze_python_file(filepath):
    """Analyzes a Python file for inputs, outputs, and calls."""
    inputs = set()
    outputs = set()
    calls = []
    objective = extract_objective_from_comments(filepath, "python")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
            analyzer = PythonAnalyzer(filepath)
            analyzer.visit(tree)
            inputs.update(analyzer.inputs)
            outputs.update(analyzer.outputs)
            calls.extend(analyzer.calls)

            # Look for sys.argv usage (simple check, argparse is more specific)
            if "sys.argv" in content and not any("argparse" in inp for inp in inputs):
                inputs.add("param_input_sysargv:sys.argv (raw)")

    except SyntaxError as e:
        print(f"Syntax error analyzing Python file {filepath}: {e}")
        objective += " (SyntaxError during analysis)"
    except Exception as e:
        print(f"Error analyzing Python file {filepath}: {e}")

    return {
        "objective": objective,
        "inputs": sorted(list(inputs)),
        "outputs": sorted(list(outputs)),
        "calls": calls
    }


# --- Bash File Analysis ---
def analyze_bash_file(filepath):
    """Analyzes a Bash file for inputs, outputs, and calls using regex."""
    inputs = set()
    outputs = set()
    calls = [] # list of dicts: {"cmd": executable, "args": list_of_args, "type": "bash_command"}
    objective = extract_objective_from_comments(filepath, "bash")
    script_dir = os.path.dirname(filepath)

    common_non_script_commands = frozenset([
        "echo", "printf", "cd", "export", "unset", "source", ".", "read", "exit",
        "set", "true", "false", "if", "then", "else", "elif", "fi", "case", "esac",
        "for", "select", "while", "until", "do", "done", "trap", "alias", "unalias",
        "let", "eval", "exec", "shift", "getopts", "wait", "test", "[", "]]", ":",
        "declare", "typeset", "local", "readonly", "pwd", "jobs", "fg", "bg", "kill",
        "return", "break", "continue", "command", "type", "hash"
    ])

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line_content in enumerate(f):
                line = line_content.strip()
                
                # Skip empty lines, comments, and simple control structures if they are the whole line
                if not line or line.startswith("#") or line in ["{", "}"]:
                    continue
                
                # Remove trailing comments like `command # this is a comment`
                line = line.split('#', 1)[0].strip()
                if not line: continue

                # Heuristic for input/output redirection affecting a file
                # command > file / command >> file
                m_out_redir = re.search(r'(>>?)\s*([\'"]?[\w\/\.-]+[\'"]?)', line)
                if m_out_redir:
                    chars_to_strip = "'\""
                    outputs.add(f"file_redirect_out:{m_out_redir.group(2).strip(chars_to_strip)}")
                                
                # command < file
                m_in_redir = re.search(r'<\s*([\'"]?[\w\/\.-]+[\'"]?)', line)
                if m_in_redir:
                    inputs.add(f"file_redirect_in:{m_in_redir.group(1).strip(chars_to_strip)}")

                # Look for commands being executed. This is tricky due to shell syntax.
                # Try to split by common delimiters like pipes, &&, ||, ; but respect quoting.
                # For simplicity, we'll use shlex for the primary command on a line if possible.
                
                # Handle pipelines: cmd1 | cmd2
                # Handle sequential: cmd1 ; cmd2
                # Handle conditional: cmd1 && cmd2, cmd1 || cmd2
                # We are interested in each individual command.
                # A simple split by these might be problematic due to quoting.
                # Instead, we'll focus on identifying the *first major command* and its args on the line.
                # More complex parsing would require a shell grammar parser.

                try:
                    # shlex.split is good for parsing a single command with its arguments
                    parts = shlex.split(line)
                except ValueError: # e.g. unclosed quote
                    parts = line.split() # Fallback to simple split

                if not parts:
                    continue

                command_exe = parts[0]
                command_args = parts[1:]

                # Filter out shell builtins and keywords unless they are part of a path
                # (e.g. ./echo is a script, echo is a builtin)
                is_path = "/" in command_exe or command_exe.startswith(".")
                if not is_path and command_exe in common_non_script_commands:
                    # Still check its arguments for potential files if it's a command that takes them
                    # e.g. `source myconfig.sh` or `cat file.txt`
                    if command_exe in ["source", "."] and command_args:
                        inputs.add(f"bash_source:{command_args[0]}")
                        # Treat sourced script as a call
                        calls.append({"cmd": command_args[0], "args": [], "type": "bash_source_call"})
                    # (could add more specific handlers for cat, grep for file arguments)
                    continue # Skip adding builtins themselves as "calls"

                # If it's not a common builtin, or it's a path, consider it a call
                calls.append({"cmd": command_exe, "args": command_args, "type": "bash_command"})

                # Heuristically identify files in arguments of the executed command
                for arg in command_args:
                    arg_clean = arg.strip("'\"")
                    # Regex for typical file extensions, or if it contains a path separator
                    if re.match(r'^[\w\.-]+\.(csv|json|txt|xml|yaml|html|png|jpg|log|gz|tar|zip)$', arg_clean, re.IGNORECASE) or \
                       (os.path.sep in arg_clean):
                        # This is a very broad heuristic. Distinguishing input vs output is hard here.
                        # Assume input unless part of an output redirection already handled or specific flags.
                        # We could add more intelligence if certain flags are known (e.g. -o outfile)
                        inputs.add(f"param_file_heuristic:{arg_clean}")


                # Positional parameters like $1, $2, $*, $@
                if re.search(r'\$[0-9@*#?]', line):
                    inputs.add("param_input_bash:positional/special_var")
                
                # Variable assignments that look like file paths (e.g., INPUT_FILE="data.csv")
                # This is simplified; doesn't handle command substitution for values robustly yet.
                var_assign_match = re.match(r'^\s*([a-zA-Z_][a-zA-Z0-9_]+)=(["\']?)(.+?)\2(?=\s|$)', line)
                if var_assign_match:
                    var_name = var_assign_match.group(1)
                    file_val = var_assign_match.group(3)
                    if re.search(r'\.(csv|json|txt|png|html|xml|yaml|log)$', file_val, re.IGNORECASE) or os.path.sep in file_val:
                        if "output" in var_name.lower() or "out" in var_name.lower() or "target" in var_name.lower():
                            outputs.add(f"file_var_assign:{file_val} (to {var_name})")
                        else:
                            inputs.add(f"file_var_assign:{file_val} (to {var_name})")
    except Exception as e:
        print(f"Error analyzing Bash file {filepath}: {e}")

    return {
        "objective": objective,
        "inputs": sorted(list(set(inputs))),
        "outputs": sorted(list(set(outputs))),
        "calls": calls
    }

# --- Service File Analysis ---
def analyze_service_file(filepath):
    """Rudimentary analysis of systemd .service files for ExecStart."""
    inputs = set() # Service files themselves don't typically take direct inputs like scripts
    outputs = set()
    calls = []
    objective = extract_objective_from_comments(filepath, "service")
    
    exec_start_cmd = None
    working_dir = None

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or not line:
                    continue
                
                match_exec = re.match(r'^\s*ExecStart\s*=\s*(.+)', line, re.IGNORECASE)
                if match_exec:
                    exec_start_cmd = match_exec.group(1)
                    # Systemd ExecStart can have prefixes like `@`, `-`, `+`, `!`, `!!`
                    # Remove these for cleaner command parsing.
                    exec_start_cmd = re.sub(r'^[@\-+!]+', '', exec_start_cmd).strip()
                    break # Assuming one primary ExecStart for simplicity

                match_wd = re.match(r'^\s*WorkingDirectory\s*=\s*(.+)', line, re.IGNORECASE)
                if match_wd:
                    working_dir = match_wd.group(1)


        if exec_start_cmd:
            # The ExecStart line is effectively a command call
            try:
                parts = shlex.split(exec_start_cmd)
                calls.append({"cmd": parts[0], "args": parts[1:], "type": "service_exec_start", "working_dir": working_dir})
            except ValueError:
                parts = exec_start_cmd.split() # Fallback
                calls.append({"cmd": parts[0], "args": parts[1:], "type": "service_exec_start_fallback_parse", "working_dir": working_dir})
            objective += f" (ExecStart: {exec_start_cmd[:100]})"
            if working_dir:
                 objective += f" (WD: {working_dir})"


    except Exception as e:
        print(f"Error analyzing service file {filepath}: {e}")

    return {
        "objective": objective,
        "inputs": sorted(list(inputs)),
        "outputs": sorted(list(outputs)),
        "calls": calls
    }


# --- Generic Command Line Tool (Placeholder for called externals) ---
def analyze_cmd_tool_call(command_name, args_passed):
    """Analyzes a call to a generic/external command line tool based on its name and args."""
    inputs = set()
    outputs = set()
    objective = f"External or generic command: {command_name}"
    if args_passed:
        objective += f" (called with: {' '.join(args_passed)[:100]})"

    if args_passed:
        for i, arg in enumerate(args_passed):
            arg_lower = arg.lower()
            next_arg = args_passed[i+1] if i+1 < len(args_passed) and not args_passed[i+1].startswith('-') else None

            is_output_flag = any(flag == arg_lower for flag in ['-o', '--output', '--out']) or \
                             any(arg_lower.startswith(f"{flag}=") for flag in ['-o', '--output', '--out'])
            is_input_flag = any(flag == arg_lower for flag in ['-i', '--input', '--in', '-f', '--file']) or \
                            any(arg_lower.startswith(f"{flag}=") for flag in ['-i', '--input', '--in', '-f', '--file'])

            value_from_flag = None
            if '=' in arg:
                try:
                    _, value_from_flag = arg.split('=', 1)
                except ValueError: pass
            
            if is_output_flag:
                val = value_from_flag or next_arg
                if val: outputs.add(f"param_output_flag:{val} (via {arg.split('=')[0]})")
            elif is_input_flag:
                val = value_from_flag or next_arg
                if val: inputs.add(f"param_input_flag:{val} (via {arg.split('=')[0]})")
            # Heuristic: standalone arguments that look like common data files (if not already claimed by a flag)
            elif not arg.startswith('-') and re.match(r'^[\w\.-]+\.(csv|json|txt|xml|yaml|html|png|jpg|log|gz|tar|zip)$', arg, re.IGNORECASE):
                # This is very ambiguous. Could be input or output.
                # If a previous arg was an output flag without a value, this might be it.
                # For now, assume input if no strong output indicator.
                inputs.add(f"param_file_heuristic:{arg}")


    return {
        "objective": objective,
        "inputs": sorted(list(inputs)),
        "outputs": sorted(list(outputs)),
        "calls": [] # External tools are leaves in this analysis context
    }

# --- Main Tree Building Logic ---
def build_program_tree(start_file_path, project_root_abs, processed_files_abs=None, max_depth=10, current_depth=0, calling_context=None):
    """
    Recursively builds a tree of program calls, inputs, and outputs.
    calling_context: dict with info about how this script was called (e.g. working_dir from service file)
    """
    if processed_files_abs is None:
        processed_files_abs = set()

    if current_depth > max_depth:
        return {"filename": os.path.basename(start_file_path), "status": "max_depth_reached", "abs_path": start_file_path}

    # Normalize path to be absolute for processing and cycle detection
    if calling_context and calling_context.get('working_dir') and not os.path.isabs(start_file_path):
        abs_script_path = os.path.abspath(os.path.join(calling_context['working_dir'], start_file_path))
    else: # If called from another script, start_file_path should be resolvable from project_root or current script's dir
         abs_script_path = os.path.abspath(start_file_path)


    if abs_script_path in processed_files_abs:
        # Create a reference node for cycles or repeated calls to already processed scripts
        display_path_ref = os.path.relpath(abs_script_path, project_root_abs) if abs_script_path.startswith(project_root_abs) else os.path.basename(abs_script_path)
        return {"filename": display_path_ref, "status": "already_processed_ref", "abs_path": abs_script_path}

    processed_files_abs.add(abs_script_path)

    # Determine display path (relative to project root if possible)
    if project_root_abs and abs_script_path.startswith(project_root_abs):
        display_path = os.path.relpath(abs_script_path, project_root_abs)
    else:
        display_path = os.path.basename(abs_script_path) # For external tools or files outside project root

    if not os.path.isfile(abs_script_path): # Check if it's a file after attempting to make it absolute
        # If it's not a file, it might be an external command name or non-existent
        # (This case is usually handled when processing 'calls' from a parent)
        return analyze_cmd_tool_call(start_file_path, calling_context.get('args', []))


    file_type, shebang_interpreter = get_file_type_and_shebang_interpreter(abs_script_path)
    
    node_data = {
        "filename": display_path,
        "abs_path": abs_script_path,
        "type": file_type,
        "shebang_interpreter": shebang_interpreter,
        "objective": "N/A",
        "inputs": [],
        "outputs": [],
        "children_calls": [] # Renamed from 'calls' to avoid confusion
    }
    
    analysis_result = None
    if file_type == "python":
        analysis_result = analyze_python_file(abs_script_path)
    elif file_type == "bash":
        analysis_result = analyze_bash_file(abs_script_path)
    elif file_type == "service":
        analysis_result = analyze_service_file(abs_script_path)
    elif file_type == "cmd_tool": # A local file we don't have a specific parser for but exists
        # Treat as a leaf for now, or could use analyze_cmd_tool_call if we have args
        # This case means build_program_tree was called directly on a generic cmd_tool
        node_data["objective"] = extract_objective_from_comments(abs_script_path, file_type) or f"Generic command/script: {display_path}"
        # No further analysis of its "calls" unless it has a shebang we understand
    elif file_type in ["python_interpreter", "bash_interpreter"]: # e.g. "python" or "bash" itself was called
        # This occurs if `python my_script.py` is a call. `python` is the cmd.
        # The actual script `my_script.py` should be the first argument.
        if calling_context and calling_context.get('args'):
            actual_script_to_run = calling_context['args'][0]
            new_calling_context = {'args': calling_context['args'][1:]} # Pass remaining args
             # The parent (caller of 'python')'s directory or project root is important context
            parent_dir_of_caller = os.path.dirname(calling_context.get('caller_abs_path', project_root_abs))
            
            # Resolve actual_script_to_run relative to project root or parent's dir
            path_options = [
                os.path.join(parent_dir_of_caller, actual_script_to_run),
                os.path.join(project_root_abs, actual_script_to_run),
                actual_script_to_run # if absolute
            ]
            resolved_actual_script = None
            for p_opt in path_options:
                if os.path.isfile(p_opt):
                    resolved_actual_script = os.path.abspath(p_opt)
                    break
            
            if resolved_actual_script:
                # Effectively replace this node with the analysis of the script passed to the interpreter
                # We decrement current_depth because this isn't a "new" level of script calling in the project logic
                return build_program_tree(resolved_actual_script, project_root_abs, processed_files_abs, max_depth, current_depth -1 , new_calling_context)
            else:
                 node_data["objective"] = f"Interpreter {file_type} called, but script '{actual_script_to_run}' not found."
                 node_data["inputs"] = [f"script_arg:{actual_script_to_run} (not found)"]

    else: # unknown_or_external or other types
        node_data["objective"] = f"Type '{file_type}' for {display_path} not specifically handled as a script."
        # It's a leaf in our analysis tree by default.

    if analysis_result:
        node_data.update(analysis_result)

    # Process calls extracted from the current file's analysis_result
    # These are in analysis_result['calls']
    child_call_list = node_data.pop('calls', []) 

    for called_item in child_call_list:
        called_program_name = called_item.get("cmd")
        called_program_args = called_item.get("args", [])
        call_type = called_item.get("type", "unknown_call")
        call_working_dir = called_item.get("working_dir") # Relevant for service files

        if not called_program_name:
            continue

        # Path resolution for the called program:
        # 1. Absolute path.
        # 2. Relative to current script's directory (abs_script_path).
        # 3. Relative to project root (project_root_abs).
        # 4. Relative to call_working_dir if provided (e.g., from service file).
        # 5. Name of an external command on PATH (handled if not found as a local file).

        current_script_dir = os.path.dirname(abs_script_path)
        paths_to_try = []
        if os.path.isabs(called_program_name):
            paths_to_try.append(called_program_name)
        
        if call_working_dir: # Highest precedence for relative paths if WD is specified
            paths_to_try.append(os.path.join(call_working_dir, called_program_name))

        paths_to_try.append(os.path.join(current_script_dir, called_program_name))
        if project_root_abs:
            paths_to_try.append(os.path.join(project_root_abs, called_program_name))
        
        # Special handling for sourced scripts in bash, they are relative to current script dir
        if call_type == "bash_source_call":
            paths_to_try = [os.path.join(current_script_dir, called_program_name)]


        resolved_called_script_path = None
        for p in paths_to_try:
            if os.path.isfile(p):
                resolved_called_script_path = os.path.abspath(p)
                break
        
        new_calling_context = {
            'args': called_program_args, 
            'caller_abs_path': abs_script_path,
            'working_dir': call_working_dir # Pass WD if this call specified one
        }

        if resolved_called_script_path:
            child_node = build_program_tree(resolved_called_script_path, project_root_abs, processed_files_abs, max_depth, current_depth + 1, new_calling_context)
        else:
            # If not found as a local file, treat as an external command or an interpreter call
            # (e.g. "python", "bash", "java", "curl")
            ft, si = get_file_type_and_shebang_interpreter(called_program_name) # Check if cmd is 'python' etc.
            if ft in ["python_interpreter", "bash_interpreter"]:
                # This means `python my_script.py` was called. `called_program_name` is "python".
                # `actual_script_to_run` is `called_program_args[0]`.
                if called_program_args:
                    child_node = build_program_tree(ft, project_root_abs, processed_files_abs, max_depth, current_depth + 1, new_calling_context)
                else:
                    child_node = analyze_cmd_tool_call(called_program_name, called_program_args)
                    child_node["objective"] = f"Interpreter {called_program_name} called without arguments."
            else:
                child_node = analyze_cmd_tool_call(called_program_name, called_program_args)
        
        node_data["children_calls"].append(child_node)

    return node_data


def main():
    parser = argparse.ArgumentParser(description="Build a dependency tree of program files, objectives, inputs, outputs, and calls.")
    parser.add_argument("start_file", help="The top-level filename to start analysis (e.g., a bash script, python script, or service file).")
    parser.add_argument("--project-root", help="Optional. The root directory of the project. Used for resolving relative paths and cleaner output display. Defaults to the directory of the start_file if not given.", default=None)
    parser.add_argument("-o", "--output", help="Output JSON file to save the tree.", default="program_tree.json")
    parser.add_argument("--max-depth", help="Maximum recursion depth for script calls.", type=int, default=15)


    args = parser.parse_args()

    # Ensure start_file path is absolute for consistency early on
    # If project_root is not given, it's often derived from start_file, so make start_file abs first.
    initial_start_file_abs = os.path.abspath(args.start_file)

    if not os.path.exists(initial_start_file_abs):
        # A special case: if the start_file itself is not a path but a command like "python"
        # This usage is less common for a "top-level file" but let's try to handle it.
        ft, _ = get_file_type_and_shebang_interpreter(args.start_file)
        if ft not in ["python_interpreter", "bash_interpreter", "unknown_or_external"]:
             print(f"Error: Start file '{args.start_file}' (resolved to '{initial_start_file_abs}') not found.")
             return
        # If it IS an interpreter or unknown external, proceed, build_program_tree will handle it.


    project_root_abs = os.path.abspath(args.project_root) if args.project_root else \
                       (os.path.dirname(initial_start_file_abs) if os.path.isfile(initial_start_file_abs) else os.getcwd())


    print(f"Starting analysis with top-level target: {args.start_file} (resolved to {initial_start_file_abs if os.path.exists(initial_start_file_abs) else 'external command'})")
    print(f"Using project root: {project_root_abs}")
    print(f"Max recursion depth: {args.max_depth}")


    # The initial call to build_program_tree.
    # If the start_file is 'python' and a script is passed as arg, need to handle this.
    # For the very first call, 'calling_context' can be minimal or assume args are from CLI.
    # However, if the user typed `python mytool.py arg1 arg2`, the tool should get `mytool.py` as start_file.
    # If they typed `mytool.sh param1`, `mytool.sh` is start_file.
    program_tree = build_program_tree(args.start_file, project_root_abs, max_depth=args.max_depth)


    try:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(program_tree, f, indent=2, ensure_ascii=False)
        print(f"\nProgram tree saved to {args.output}")
    except Exception as e:
        print(f"\nError saving program tree to JSON: {e}")
        print("\n--- Raw Program Tree (stdout) ---")
        import pprint
        pprint.pprint(program_tree)


if __name__ == "__main__":
    main()