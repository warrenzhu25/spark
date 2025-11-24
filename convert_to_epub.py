#!/usr/bin/env python3
"""
Spark SQL Source Code to EPUB Converter

This script converts all Spark SQL source code (Catalyst + Core modules)
into a comprehensive EPUB book, removing license headers and import statements.
"""

import os
import re
from pathlib import Path
from collections import defaultdict
import subprocess

# Book structure definition
BOOK_STRUCTURE = {
    "PART I: Foundation": {
        "Chapter 1: Core Abstractions": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/types",
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala",
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees",
        ],
        "Chapter 2: Configuration & Context": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala",
            "sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala",
        ],
        "Chapter 3: Error Handling": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/errors",
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreePatternBits.scala",
        ],
    },
    "PART II: SQL Parsing & Analysis": {
        "Chapter 4: SQL Parsing": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser",
        ],
        "Chapter 5: Query Analysis": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis",
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog",
        ],
        "Chapter 6: Expressions": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions",
        ],
    },
    "PART III: Query Planning": {
        "Chapter 7: Logical Plans": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical",
        ],
        "Chapter 8: Query Optimization": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer",
        ],
        "Chapter 9: Physical Planning": [
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanner.scala",
        ],
    },
    "PART IV: Execution Engine": {
        "Chapter 10: Execution Framework": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/metric",
        ],
        "Chapter 11: Core Operators": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/joins",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/window",
        ],
        "Chapter 12: Data Sources": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/datasources",
            "sql/catalyst/src/main/scala/org/apache/spark/sql/connector",
        ],
        "Chapter 13: Adaptive Execution": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive",
        ],
        "Chapter 14: Advanced Features": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/columnar",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/exchange",
        ],
    },
    "PART V: Streaming & Advanced": {
        "Chapter 15: Structured Streaming": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/streaming",
        ],
        "Chapter 16: Stateful Operations": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state",
        ],
        "Chapter 17: Language Integration": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/python",
            "sql/core/src/main/scala/org/apache/spark/sql/execution/r",
        ],
    },
    "PART VI: User-Facing APIs": {
        "Chapter 18: DataFrame/Dataset API": [
            "sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala",
            "sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala",
        ],
        "Chapter 19: SQL Commands": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/command",
        ],
        "Chapter 20: UI & Observability": [
            "sql/core/src/main/scala/org/apache/spark/sql/execution/ui",
        ],
    },
}


def remove_license_header(content):
    """Remove Apache license header from content."""
    # Match the standard Apache license header (17 lines including blank line)
    license_pattern = r'^/\*\s*\n \* Licensed to the Apache Software Foundation.*?\*/\s*\n'
    content = re.sub(license_pattern, '', content, flags=re.DOTALL)
    return content


def remove_imports(content):
    """Remove import statements from Scala code."""
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


def process_scala_file(file_path):
    """Process a single Scala file, removing licenses and imports."""
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


def get_scala_files(path_patterns, base_dir):
    """Get all Scala files matching the path patterns."""
    files = []

    for pattern in path_patterns:
        full_pattern = os.path.join(base_dir, pattern)

        if os.path.isfile(full_pattern):
            # Single file
            files.append(full_pattern)
        elif os.path.isdir(full_pattern):
            # Directory - recursively find all .scala files
            path_obj = Path(full_pattern)
            scala_files = sorted(path_obj.rglob('*.scala'))
            files.extend([str(f) for f in scala_files])

    return sorted(set(files))


def generate_markdown(base_dir):
    """Generate complete markdown content from all Scala files."""
    markdown = []

    # Title
    markdown.append("# Apache Spark SQL: Complete Source Code\n\n")
    markdown.append("A comprehensive reference of the Apache Spark SQL source code, ")
    markdown.append("including the Catalyst query optimization framework and ")
    markdown.append("execution engine.\n\n")
    markdown.append("---\n\n")

    # Table of contents
    markdown.append("# Table of Contents\n\n")
    for part_name, chapters in BOOK_STRUCTURE.items():
        markdown.append(f"## {part_name}\n\n")
        for chapter_name in chapters.keys():
            # Create anchor-safe chapter ID
            chapter_id = chapter_name.lower().replace(' ', '-').replace(':', '').replace('&', 'and')
            markdown.append(f"- [{chapter_name}](#{chapter_id})\n")
        markdown.append("\n")

    markdown.append("---\n\n")

    # Process each part and chapter
    total_files = 0
    for part_name, chapters in BOOK_STRUCTURE.items():
        markdown.append(f"# {part_name}\n\n")
        markdown.append("---\n\n")

        for chapter_name, path_patterns in chapters.items():
            markdown.append(f"## {chapter_name}\n\n")

            # Get all Scala files for this chapter
            scala_files = get_scala_files(path_patterns, base_dir)

            if not scala_files:
                markdown.append("*No files found for this chapter.*\n\n")
                continue

            markdown.append(f"*This chapter contains {len(scala_files)} source files.*\n\n")
            total_files += len(scala_files)

            # Process each file
            for file_path in scala_files:
                # Get relative path for display
                rel_path = os.path.relpath(file_path, base_dir)
                file_name = os.path.basename(file_path)

                markdown.append(f"### {file_name}\n\n")
                markdown.append(f"**Path**: `{rel_path}`\n\n")

                # Process file content
                content = process_scala_file(file_path)

                if content:
                    # Add code block with syntax highlighting
                    markdown.append("```scala\n")
                    markdown.append(content)
                    markdown.append("\n```\n\n")
                else:
                    markdown.append("*Error processing file*\n\n")

                markdown.append("---\n\n")

            markdown.append("\n\n")

    markdown.append(f"\n\n---\n\n**Total files processed**: {total_files}\n")

    return ''.join(markdown)


def convert_markdown_to_epub(markdown_file, output_epub, metadata):
    """Convert markdown to EPUB using pandoc."""

    # Create metadata file
    metadata_content = f"""---
title: "{metadata['title']}"
author: "{metadata['author']}"
date: "{metadata['date']}"
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
    except subprocess.CalledProcessError as e:
        print(f"Error creating EPUB: {e}")
    except FileNotFoundError:
        print("Error: pandoc not found. Please install pandoc:")
        print("  brew install pandoc  # on macOS")
        print("  apt-get install pandoc  # on Ubuntu/Debian")
    finally:
        # Clean up metadata file
        if os.path.exists(metadata_file):
            os.remove(metadata_file)


def main():
    """Main conversion function."""
    import datetime

    # Base directory (spark repository root)
    base_dir = '/Users/warren/github/spark-me'

    # Output files
    markdown_file = 'spark-sql-complete-source.md'
    epub_file = 'spark-sql-complete-source.epub'

    print("Spark SQL Source Code to EPUB Converter")
    print("=" * 50)
    print(f"Base directory: {base_dir}")
    print(f"Output markdown: {markdown_file}")
    print(f"Output EPUB: {epub_file}")
    print()

    # Generate markdown
    print("Generating markdown from Scala sources...")
    markdown_content = generate_markdown(base_dir)

    # Write markdown file
    print(f"Writing markdown to {markdown_file}...")
    with open(markdown_file, 'w', encoding='utf-8') as f:
        f.write(markdown_content)

    print(f"Markdown file created: {os.path.getsize(markdown_file) / 1024 / 1024:.2f} MB")
    print()

    # Convert to EPUB
    print("Converting to EPUB...")
    metadata = {
        'title': 'Apache Spark SQL: Complete Source Code',
        'author': 'Apache Spark Contributors',
        'date': datetime.datetime.now().strftime('%Y-%m-%d'),
    }

    convert_markdown_to_epub(markdown_file, epub_file, metadata)

    if os.path.exists(epub_file):
        print(f"EPUB file created: {os.path.getsize(epub_file) / 1024 / 1024:.2f} MB")

    print()
    print("Conversion complete!")
    print(f"Markdown: {markdown_file}")
    print(f"EPUB: {epub_file}")


if __name__ == '__main__':
    main()
