#!/usr/bin/env python3
import json

# Load discovered files
with open('/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard/output/discovered_files_20250625_171007.json', 'r') as f:
    data = json.load(f)

print(f'📊 Discovery Results Summary')
print(f'Total Excel files found: {len(data)}')
print()

# Deal stages breakdown
print('📁 Files by Deal Stage:')
stages = {}
for file in data:
    stage = file['deal_stage']
    stages[stage] = stages.get(stage, 0) + 1

for stage, count in sorted(stages.items()):
    print(f'  • {stage}: {count} files')

print()

# Recent files
print('🕐 Most Recently Modified Files:')
recent = sorted(data, key=lambda x: x['last_modified'], reverse=True)[:10]
for file in recent:
    print(f'  • {file["file_name"]}')
    print(f'    Deal: {file["deal_name"]}')
    print(f'    Modified: {file["last_modified"][:10]}')
    print(f'    Size: {file["size_mb"]:.1f} MB')
    print()

# File size summary
total_size = sum(f['size_mb'] for f in data)
print(f'📦 Total Data Size: {total_size:.1f} MB')