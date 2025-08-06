#!/bin/bash

echo "DEBUG: Starting debug wrapper"
echo "DEBUG: Current shell: $SHELL"
echo "DEBUG: Bash version: $BASH_VERSION"
echo "DEBUG: Environment variables containing 'bash':"
env | grep -i bash

# Run the actual script with strace to catch any exec calls
echo "DEBUG: Running with strace to catch exec calls..."
strace -e execve -f -o /tmp/rpc_strace.log bash /home/smilax/trillium_api/scripts/get_slots/rpc_get_block_data.sh "$@"

echo "DEBUG: Script completed. Checking strace log for 'environment' calls..."
grep -i environment /tmp/rpc_strace.log || echo "No 'environment' calls found in strace"