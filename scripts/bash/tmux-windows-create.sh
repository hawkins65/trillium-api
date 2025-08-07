#!/bin/bash

# Simple tmux session selector with menu
# Shows all active tmux sessions and lets you pick one to attach to

# Function to display menu and get user choice
show_menu() {
    echo "===================="
    echo "  TMUX SESSIONS"
    echo "===================="
    echo
    
    local sessions=("$@")
    local count=1
    
    for session in "${sessions[@]}"; do
        echo "[$count] $session"
        ((count++))
    done
    
    echo
    echo "[q] Quit"
    echo
    echo -n "Select a session (1-$((count-1)) or q): "
}

# Get list of tmux sessions
sessions=$(tmux list-sessions -F "#{session_name}" 2>/dev/null)

# Check if tmux is running and has sessions
if [ $? -ne 0 ] || [ -z "$sessions" ]; then
    echo "❌ No tmux server is running or no sessions found"
    echo "💡 Start tmux with: tmux new-session -s mysession"
    exit 1
fi

# Convert sessions string to array
readarray -t session_array <<< "$sessions"

# Show current sessions info
echo "Found ${#session_array[@]} active tmux session(s):"
echo

# Main menu loop
while true; do
    show_menu "${session_array[@]}"
    read -r choice
    
    case $choice in
        q|Q)
            echo "👋 Goodbye!"
            exit 0
            ;;
        ''|*[!0-9]*)
            echo "❌ Invalid input. Please enter a number or 'q'"
            echo
            ;;
        *)
            if [ "$choice" -ge 1 ] && [ "$choice" -le "${#session_array[@]}" ]; then
                selected_session="${session_array[$((choice-1))]}"
                echo
                echo "🔗 Attaching to session: $selected_session"
                echo "💡 Press Ctrl+A then D to detach later"
                echo
                sleep 1
                tmux attach-session -t "$selected_session"
                exit 0
            else
                echo "❌ Invalid choice. Please select 1-${#session_array[@]} or 'q'"
                echo
            fi
            ;;
    esac
done