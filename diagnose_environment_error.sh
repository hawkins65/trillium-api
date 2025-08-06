#!/bin/bash

echo "=== Diagnostic Script for Environment Error ==="
echo

echo "1. Checking for 'environment' command/alias/function:"
type -a environment 2>/dev/null || echo "   Not found in current shell"
echo

echo "2. Checking PATH for 'environment' executable:"
which environment 2>/dev/null || echo "   Not found in PATH"
echo

echo "3. Checking for shell functions:"
declare -F | grep -i environment || echo "   No environment-related functions"
echo

echo "4. Current argument count and values:"
echo "   Args: $#"
echo "   Values: [$*]"
echo

echo "5. Checking BASH_ENV:"
echo "   BASH_ENV=${BASH_ENV:-<not set>}"
echo

echo "6. Checking for DEBUG trap:"
trap -p DEBUG || echo "   No DEBUG trap set"
echo

echo "7. Testing direct execution of rpc_get_block_data.sh:"
echo "   Command: bash /home/smilax/trillium_api/scripts/get_slots/rpc_get_block_data.sh 826"
echo "   (Not executing to avoid long-running process)"
echo

echo "8. Shell version:"
echo "   $BASH_VERSION"
echo

echo "=== Recommendations ==="
echo "1. Try running with explicit bash and no rc files:"
echo "   bash --norc --noprofile /home/smilax/trillium_api/scripts/bash/90_get_block_data.sh 826"
echo
echo "2. Check if the error occurs in a clean environment:"
echo "   env -i bash /home/smilax/trillium_api/scripts/bash/90_get_block_data.sh 826"
echo
echo "3. The 'environment: line 19' error suggests a script called 'environment' is being executed"
echo "   with invalid syntax at line 19. This could be from:"
echo "   - A shell hook in your IDE"
echo "   - A custom script in your PATH"
echo "   - An alias or function definition"
echo

echo "=== To find the problematic 'environment' script ==="
echo "Run these commands:"
echo "   find /usr/local/bin /usr/bin ~/.local/bin -name 'environment' 2>/dev/null"
echo "   grep -r 'environment' ~/.bashrc ~/.bash_profile ~/.profile 2>/dev/null"