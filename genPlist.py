import os
import plistlib

def generate_poetry_plist(label: str, project_path: str, script_path: str):
    project_path = os.path.expanduser(project_path)
    script_path = os.path.join(project_path, script_path)

    plist = {
        "Label": label,
        "ProgramArguments": [
            "~/.local/bin/poetry", "run", "python", script_path
        ],
        "WorkingDirectory": project_path,
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": f"/tmp/{label}.log",
        "StandardErrorPath": f"/tmp/{label}.err",
        "EnvironmentVariables": {
            "PATH": os.environ["PATH"],  # 确保能找到 poetry 和 python
            "POETRY_VIRTUALENVS_CREATE": "false"  # 可选：强制使用本地虚拟环境
        }
    }

    plist_path = os.path.expanduser(f"~/Library/LaunchAgents/{label}.plist")
    os.makedirs(os.path.dirname(plist_path), exist_ok=True)
    with open(plist_path, "wb") as f:
        plistlib.dump(plist, f)

    print(f"✅ Plist written to {plist_path}")
    print(f"👉 To load: launchctl load {plist_path}")
    print(f"👉 To start: launchctl start {label}")


if __name__ == "__main__":

    generate_poetry_plist(
        label="com.example.graph-poetry",
        project_path="~/startup/rpc_feed",
        script_path="tests/test_graph.py"
    )
