import ast
from pathlib import Path

PROJECT = Path(".")

print("\n" + "=" * 70)
print("PROJECT MAP")
print("=" * 70)

for py in sorted(PROJECT.glob("*.py")):

    print(f"\n📄 {py.name}")

    try:
        tree = ast.parse(py.read_text(encoding="utf-8"))

    except Exception as e:
        print("   ERROR:", e)
        continue

    funcs = []
    classes = []

    for node in tree.body:

        if isinstance(node, ast.FunctionDef):
            funcs.append((node.lineno, node.name))

        elif isinstance(node, ast.ClassDef):
            classes.append((node.lineno, node.name))

    if classes:
        print("  Classes:")
        for line, name in classes:
            print(f"    {line:>5}  {name}")

    if funcs:
        print("  Functions:")
        for line, name in funcs:
            print(f"    {line:>5}  {name}")

print("\n" + "=" * 70)
