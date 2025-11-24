#!/usr/bin/env python3
"""
Generic Scala/Java Source Code to EPUB Converter

This script converts any folder of Scala/Java source files into an EPUB book,
removing license headers and import statements.

Usage:
    python3 convert_folder_to_epub.py <source_dir> <output_name> [--title "Book Title"]

Examples:
    # Convert Spark Core
    python3 convert_folder_to_epub.py core/src/main/scala spark-core "Spark Core Source"

    # Convert Spark Streaming
    python3 convert_folder_to_epub.py streaming/src/main/scala spark-streaming "Spark Streaming Source"

    # Convert MLlib
    python3 convert_folder_to_epub.py mllib/src/main/scala spark-mllib "Spark MLlib Source"
"""

import os
import re
import sys
import argparse
from pathlib import Path
from collections import defaultdict
import subprocess
import datetime


def remove_license_header(content):
    """Remove common license headers from content."""
    # Apache license
    apache_pattern = r'^/\*\s*\n \* Licensed to the Apache Software Foundation.*?\*/\s*\n'
    content = re.sub(apache_pattern, '', content, flags=re.DOTALL)

    # MIT license
    mit_pattern = r'^/\*\s*\n \* MIT License.*?\*/\s*\n'
    content = re.sub(mit_pattern, '', content, flags=re.DOTALL)

    # BSD license
    bsd_pattern = r'^/\*\s*\n \* Copyright.*?BSD.*?\*/\s*\n'
    content = re.sub(bsd_pattern, '', content, flags=re.DOTALL)

    # Generic copyright
    copyright_pattern = r'^/\*\s*\n \* Copyright.*?\*/\s*\n'
    content = re.sub(copyright_pattern, '', content, flags=re.DOTALL)

    return content


def remove_imports(content):
    """Remove import statements from Scala/Java code."""
    lines = content.split('\n')
    result = []
    in_imports = False

    for line in lines:
        stripped = line.strip()

        # Skip import statements
        if stripped.startswith('import '):
            in_imports = True
            continue
        elif in_imports and not stripped:
            # Skip blank lines after imports
            in_imports = False
            continue
        elif in_imports and stripped.startswith('import '):
            continue
        else:
            in_imports = False
            result.append(line)

    return '\n'.join(result)


def clean_excessive_blank_lines(content):
    """Reduce excessive blank lines to maximum 2 consecutive."""
    return re.sub(r'\n{4,}', '\n\n\n', content)


def process_source_file(file_path):
    """Process a single source file, removing licenses and imports."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Remove license header
        content = remove_license_header(content)

        # Remove imports
        content = remove_imports(content)

        # Clean excessive blank lines
        content = clean_excessive_blank_lines(content)

        return content.strip()
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None


def organize_files_by_package(files):
    """Organize files by their package/directory structure."""
    structure = defaultdict(list)

    for file_path in files:
        # Get the directory path
        dir_path = os.path.dirname(file_path)
        dir_name = os.path.basename(dir_path)

        if not dir_name:
            dir_name = "Root"

        structure[dir_name].append(file_path)

    return structure


def get_source_files(source_dir, extensions=None):
    """Get all source files in the directory."""
    if extensions is None:
        extensions = ['.scala', '.java']

    files = []
    source_path = Path(source_dir)

    for ext in extensions:
        files.extend(sorted(source_path.rglob(f'*{ext}')))

    return [str(f) for f in files]


def detect_language(file_path):
    """Detect the programming language from file extension."""
    ext = os.path.splitext(file_path)[1].lower()

    language_map = {
        '.scala': 'scala',
        '.java': 'java',
        '.py': 'python',
        '.cpp': 'cpp',
        '.cc': 'cpp',
        '.h': 'cpp',
        '.hpp': 'cpp',
        '.c': 'c',
        '.go': 'go',
        '.rs': 'rust',
        '.kt': 'kotlin',
    }

    return language_map.get(ext, 'text')


def generate_markdown(source_dir, title, base_dir=None):
    """Generate complete markdown content from all source files."""
    if base_dir is None:
        base_dir = os.getcwd()

    markdown = []

    # Title
    markdown.append(f"# {title}\n\n")
    markdown.append(f"Complete source code from `{source_dir}`\n\n")
    markdown.append("---\n\n")

    # Get all source files
    source_files = get_source_files(source_dir)

    if not source_files:
        print(f"No source files found in {source_dir}")
        return None

    print(f"Found {len(source_files)} source files")

    # Organize by package/directory
    structure = organize_files_by_package(source_files)

    # Table of contents
    markdown.append("# Table of Contents\n\n")
    for package_name in sorted(structure.keys()):
        markdown.append(f"- [{package_name}](#{package_name.lower().replace(' ', '-')})\n")
    markdown.append("\n---\n\n")

    # Process each package
    total_files = 0
    for package_name in sorted(structure.keys()):
        files = sorted(structure[package_name])

        markdown.append(f"# {package_name}\n\n")
        markdown.append(f"*This section contains {len(files)} source files.*\n\n")
        total_files += len(files)

        # Process each file
        for file_path in files:
            file_name = os.path.basename(file_path)
            rel_path = os.path.relpath(file_path, base_dir)
            language = detect_language(file_path)

            markdown.append(f"## {file_name}\n\n")
            markdown.append(f"**Path**: `{rel_path}`\n\n")

            # Process file content
            content = process_source_file(file_path)

            if content:
                # Add code block with syntax highlighting
                markdown.append(f"```{language}\n")
                markdown.append(content)
                markdown.append("\n```\n\n")
            else:
                markdown.append("*Error processing file*\n\n")

            markdown.append("---\n\n")

    markdown.append(f"\n\n**Total files processed**: {total_files}\n")

    return ''.join(markdown)


def convert_markdown_to_epub(markdown_file, output_epub, metadata):
    """Convert markdown to EPUB using pandoc."""
    # Create metadata file
    metadata_content = f"""---
title: "{metadata['title']}"
author: "{metadata.get('author', 'Unknown')}"
date: "{metadata.get('date', datetime.datetime.now().strftime('%Y-%m-%d'))}"
lang: en-US
toc: true
toc-depth: 3
---
"""

    metadata_file = 'book_metadata.yaml'
    with open(metadata_file, 'w') as f:
        f.write(metadata_content)

    # Build pandoc command
    cmd = [
        'pandoc',
        markdown_file,
        '-o', output_epub,
        '--metadata-file=' + metadata_file,
        '--toc',
        '--toc-depth=3',
        '--syntax-highlighting=tango',
        '--standalone'
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"Successfully created EPUB: {output_epub}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error creating EPUB: {e}")
        return False
    except FileNotFoundError:
        print("Error: pandoc not found. Please install pandoc:")
        print("  brew install pandoc  # on macOS")
        print("  apt-get install pandoc  # on Ubuntu/Debian")
        return False
    finally:
        # Clean up metadata file
        if os.path.exists(metadata_file):
            os.remove(metadata_file)


def main():
    """Main conversion function."""
    parser = argparse.ArgumentParser(
        description='Convert source code directory to EPUB book',
        epilog='Example: python3 convert_folder_to_epub.py core/src/main/scala spark-core "Spark Core"'
    )
    parser.add_argument('source_dir', help='Source directory to convert')
    parser.add_argument('output_name', help='Output file name (without extension)')
    parser.add_argument('--title', default=None, help='Book title')
    parser.add_argument('--author', default='Open Source Contributors', help='Book author')
    parser.add_argument('--extensions', default='.scala,.java', help='File extensions to include (comma-separated)')

    args = parser.parse_args()

    # Validate source directory
    if not os.path.exists(args.source_dir):
        print(f"Error: Source directory '{args.source_dir}' not found")
        return 1

    # Set title
    title = args.title if args.title else f"Source Code: {os.path.basename(args.source_dir)}"

    # Output files
    markdown_file = f"{args.output_name}.md"
    epub_file = f"{args.output_name}.epub"

    print("Source Code to EPUB Converter")
    print("=" * 50)
    print(f"Source directory: {args.source_dir}")
    print(f"Title: {title}")
    print(f"Output markdown: {markdown_file}")
    print(f"Output EPUB: {epub_file}")
    print()

    # Generate markdown
    print("Generating markdown from source files...")
    markdown_content = generate_markdown(args.source_dir, title)

    if not markdown_content:
        print("Error: No content generated")
        return 1

    # Write markdown file
    print(f"Writing markdown to {markdown_file}...")
    with open(markdown_file, 'w', encoding='utf-8') as f:
        f.write(markdown_content)

    print(f"Markdown file created: {os.path.getsize(markdown_file) / 1024 / 1024:.2f} MB")
    print()

    # Convert to EPUB
    print("Converting to EPUB...")
    metadata = {
        'title': title,
        'author': args.author,
        'date': datetime.datetime.now().strftime('%Y-%m-%d'),
    }

    success = convert_markdown_to_epub(markdown_file, epub_file, metadata)

    if success and os.path.exists(epub_file):
        print(f"EPUB file created: {os.path.getsize(epub_file) / 1024 / 1024:.2f} MB")
        print()
        print("Conversion complete!")
        print(f"Markdown: {markdown_file}")
        print(f"EPUB: {epub_file}")
        return 0
    else:
        print("Error: EPUB conversion failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
