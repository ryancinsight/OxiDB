#!/usr/bin/env python3
"""
Script to fix unused variable warnings in Rust code by prefixing them with underscores.
"""

import re
import sys

def fix_unused_variables(file_path):
    """Fix unused variables in a Rust file by prefixing them with underscores."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Pattern to match variable declarations
        patterns = [
            # let variable = ...
            (r'(\s+let\s+)([a-zA-Z_][a-zA-Z0-9_]*)\s*=', r'\1_\2 ='),
        ]
        
        original_content = content
        
        for pattern, replacement in patterns:
            content = re.sub(pattern, replacement, content)
        
        # Only write if content changed
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            print(f"Fixed unused variables in {file_path}")
            return True
        else:
            print(f"No changes needed in {file_path}")
            return False
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    files_to_fix = [
        'examples/mongodb_style_document_demo.rs',
        'examples/advanced_integration_tests.rs'
    ]
    
    total_fixed = 0
    for file_path in files_to_fix:
        if fix_unused_variables(file_path):
            total_fixed += 1
    
    print(f"\nFixed {total_fixed} files")

if __name__ == "__main__":
    main()