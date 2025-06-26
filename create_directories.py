"""
Create all required directories for the B&R Capital Dashboard project
Run this to ensure all necessary directories exist
"""

import os
from pathlib import Path

def create_project_directories():
    """Create all required directories for the project"""
    
    # Get project root
    project_root = Path.cwd()
    print(f"Project root: {project_root}")
    
    # Define all required directories
    directories = [
        "data",
        "data/extractions",
        "data/raw",
        "data/processed",
        "logs",
        "output",
        "output/reports",
        "output/exports",
        "temp"
    ]
    
    print("\nCreating directories...")
    created = 0
    existing = 0
    
    for dir_path in directories:
        full_path = project_root / dir_path
        if full_path.exists():
            print(f"✓ Already exists: {dir_path}")
            existing += 1
        else:
            full_path.mkdir(parents=True, exist_ok=True)
            print(f"✓ Created: {dir_path}")
            created += 1
    
    print(f"\nSummary: Created {created} directories, {existing} already existed")
    
    # Create .gitkeep files in data directories (so they're tracked by git)
    gitkeep_dirs = ["data/extractions", "data/raw", "data/processed", "logs", "output/reports", "output/exports", "temp"]
    
    print("\nAdding .gitkeep files...")
    for dir_path in gitkeep_dirs:
        gitkeep_path = project_root / dir_path / ".gitkeep"
        if not gitkeep_path.exists():
            gitkeep_path.write_text("# This file ensures the directory is tracked by git\n")
            print(f"✓ Added .gitkeep to {dir_path}")
    
    print("\nAll directories are ready!")
    print("\nYou can now run:")
    print("  python -m tests.excel_extraction_test")


if __name__ == "__main__":
    print("B&R Capital Dashboard - Directory Setup")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("src").exists():
        print("ERROR: 'src' folder not found!")
        print("Please run this script from your project root:")
        print("  cd /home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard")
        print("  python create_directories.py")
    else:
        create_project_directories()