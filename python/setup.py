#!/usr/bin/env python3
"""Setup script for Sleeper Python package with Rust native library integration."""

import platform
import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import setup, find_packages
from setuptools.command.build_py import build_py as _build_py


class BuildWithCtypesgen(_build_py):
    """Custom build command that generates ctypes bindings from C headers."""

    def run(self):
        self.generate_sleeper_df_bindings()
        super().run()

    def generate_sleeper_df_bindings(self):
        """Generate Python ctypes bindings from sleeper_df.h using ctypesgen."""
        print("Generating ctypes bindings for sleeper_df...")

        # Detect host architecture
        machine = platform.machine()
        if machine == "x86_64":
            rust_target = "x86_64-unknown-linux-gnu"
        elif machine in ("aarch64", "arm64"):
            rust_target = "aarch64-unknown-linux-gnu"
        else:
            raise ValueError(f"Unsupported architecture: {machine}")

        # Find paths
        project_root = Path(__file__).parent.parent.absolute()
        rust_dir = project_root / "rust"
        header_file = rust_dir / "sleeper_df" / "include" / "sleeper_df.h"
        lib_dir = rust_dir / "target" / rust_target / "release"
        lib_file = lib_dir / "libsleeper_df.so"

        if not header_file.exists():
            raise FileNotFoundError(f"C header not found: {header_file}")
        if not lib_file.exists():
            raise FileNotFoundError(
                f"Native library not found: {lib_file}\n"
                f"Please run: ./scripts/build/build.sh"
            )

        # Output paths
        bindings_module_dir = Path(__file__).parent / "src" / "sleeper" / "generated"
        bindings_module_dir.mkdir(exist_ok=True, parents=True)
        bindings_file = bindings_module_dir / "sleeper_df_bindings.py"
        lib_copy = bindings_module_dir / "libsleeper_df.so"

        # Copy native library
        print(f"Copying native library from {lib_file} to {lib_copy}")
        shutil.copy2(lib_file, lib_copy)

        # Generate bindings with ctypesgen
        try:
            from ctypesgen.main import main as ctypesgen_main
        except ImportError:
            print("Installing ctypesgen...")
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "ctypesgen"]
            )
            from ctypesgen.main import main as ctypesgen_main

        print(f"Generating bindings from {header_file}")

        # Use ctypesgen.main.main() with proper argument handling
        old_argv = sys.argv
        try:
            sys.argv = [
                "ctypesgen",
                "-o",
                str(bindings_file),
                "-l",
                "libsleeper_df.so",
                str(header_file),
            ]
            ctypesgen_main()
        finally:
            sys.argv = old_argv

        print(f"Bindings generated at {bindings_file}")

        # Post-process bindings to fix enum name collision with c_char_p
        self._fix_bindings_enum_collision(bindings_file)

        # Create __init__.py for generated package
        init_file = bindings_module_dir / "__init__.py"
        init_file.write_text("# Auto-generated bindings for sleeper_df\n")

    def _fix_bindings_enum_collision(self, bindings_file):
        """Fix ctypesgen issue where enum 'String' collides with const char* fields."""
        import re

        with open(bindings_file, 'r') as f:
            content = f.read()

        # Replace 'String' used as a type (in struct field definitions) with 'c_char_p'
        # Match patterns like ('fieldname', String) and replace String with c_char_p
        content = re.sub(
            r"\(\s*'([^']+)',\s+String\s*\)",
            r"('\1', c_char_p)",
            content
        )

        with open(bindings_file, 'w') as f:
            f.write(content)


if __name__ == "__main__":
    setup(
        cmdclass={"build_py": BuildWithCtypesgen},
    )
