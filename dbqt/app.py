import sys
import os
import importlib
from pathlib import Path


def discover_tools():
    """Discover available tools by scanning the tools directory."""
    tools_dir = Path(__file__).parent / "tools"
    tools = {}

    for file_path in tools_dir.glob("*.py"):
        if file_path.name.startswith("__"):
            continue

        tool_name = file_path.stem
        # Convert underscores to hyphens for command names
        command_name = tool_name.replace("_", "-")
        tools[command_name] = tool_name

    return tools


def get_tool_aliases():
    """Get tool aliases from tools.__init__.py if available."""
    try:
        from dbqt.tools import TOOL_ALIASES

        return TOOL_ALIASES
    except ImportError:
        return {}


def get_available_commands():
    """Get all available commands including aliases."""
    tools = discover_tools()
    aliases = get_tool_aliases()

    # Combine tools and aliases
    all_commands = {}
    all_commands.update(tools)
    all_commands.update(aliases)

    return all_commands, tools, aliases


def generate_help_text(tools, aliases):
    """Generate dynamic help text based on discovered tools."""
    help_lines = [
        "Database Query Tools (dbqt)",
        "",
        "Usage: dbqt <command> [args...]",
        "",
        "Commands:",
    ]

    # Create reverse mapping of tool to aliases
    tool_to_aliases = {}
    for alias, tool in aliases.items():
        if tool not in tool_to_aliases:
            tool_to_aliases[tool] = []
        tool_to_aliases[tool].append(alias)

    # Add main tools with their aliases
    for command, tool in sorted(tools.items()):
        command_list = [command]
        if tool in tool_to_aliases:
            command_list.extend(sorted(tool_to_aliases[tool]))

        commands_str = ", ".join(command_list)
        help_lines.append(f"  {commands_str:<20} Tool: {tool}")

    help_lines.extend(
        ["", "Run 'dbqt <command> --help' for detailed help on each command."]
    )

    return "\n".join(help_lines)


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ["-h", "--help"]:
        all_commands, tools, aliases = get_available_commands()
        print(generate_help_text(tools, aliases))
        sys.exit(1)

    command = sys.argv[1]
    args = sys.argv[2:]

    all_commands, tools, aliases = get_available_commands()

    if command not in all_commands:
        print(f"Unknown command: {command}")
        print(f"Available commands: {', '.join(sorted(all_commands.keys()))}")
        sys.exit(1)

    # Get the actual tool name (resolve aliases)
    tool_name = all_commands[command]

    try:
        # Import the tool module
        module = importlib.import_module(f"dbqt.tools.{tool_name}")

        # Determine the entry point function
        if hasattr(module, "main"):
            entry_point = module.main
        elif tool_name == "colcompare" and hasattr(module, "colcompare"):
            entry_point = module.colcompare
        else:
            print(
                f"Error: Tool '{tool_name}' does not have a main() or {tool_name}() function"
            )
            sys.exit(1)

        # Run the tool
        result = entry_point(args)
        if result is not None:
            sys.exit(result)

    except ImportError as e:
        print(f"Error importing tool '{tool_name}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error running tool '{tool_name}': {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
