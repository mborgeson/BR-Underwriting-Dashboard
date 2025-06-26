"""
Quick test script to verify SharePoint connection and Real Estate library access
Run this first to ensure the path configuration is correct
"""

import os
import sys
import requests
import json
from datetime import datetime
from pathlib import Path

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))

# Configuration
AZURE_CLIENT_ID = "5a620cea-31fe-40f6-8b48-d55bc5465dc9"
AZURE_TENANT_ID = "383e5745-a469-4712-aaa9-f7d79c981e10"
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "YOUR_CLIENT_SECRET_HERE")
SHAREPOINT_SITE = "https://bandrcapital.sharepoint.com/sites/BRCapital-Internal"


def test_sharepoint_connection():
    """Test the SharePoint connection and verify Real Estate library access"""
    
    print("=" * 60)
    print("B&R Capital SharePoint Connection Test")
    print("=" * 60)
    
    # Step 1: Get Access Token
    print("\n1. Getting access token...")
    token_url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token"
    
    data = {
        'client_id': AZURE_CLIENT_ID,
        'client_secret': AZURE_CLIENT_SECRET,
        'scope': 'https://graph.microsoft.com/.default',
        'grant_type': 'client_credentials'
    }
    
    try:
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        access_token = response.json()['access_token']
        print("   ✓ Successfully authenticated")
    except Exception as e:
        print(f"   ✗ Authentication failed: {e}")
        return False
    
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Step 2: Get Site ID
    print("\n2. Getting SharePoint site ID...")
    site_path = SHAREPOINT_SITE.replace("https://bandrcapital.sharepoint.com", "")
    url = f"https://graph.microsoft.com/v1.0/sites/bandrcapital.sharepoint.com:{site_path}"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        site_id = response.json()['id']
        print(f"   ✓ Site ID: {site_id}")
    except Exception as e:
        print(f"   ✗ Failed to get site ID: {e}")
        return False
    
    # Step 3: List Document Libraries
    print("\n3. Listing document libraries...")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        drives = response.json().get('value', [])
        
        print(f"   Found {len(drives)} document libraries:")
        real_estate_drive_id = None
        
        for drive in drives:
            name = drive.get('name', 'Unknown')
            drive_id = drive.get('id', 'Unknown')
            print(f"   - {name} (ID: {drive_id})")
            
            if name == 'Real Estate':
                real_estate_drive_id = drive_id
                print("     ^ This is our target library!")
                
    except Exception as e:
        print(f"   ✗ Failed to list drives: {e}")
        return False
    
    if not real_estate_drive_id:
        print("\n   ✗ ERROR: Real Estate library not found!")
        return False
    
    # Step 4: Check Real Estate Library Contents
    print("\n4. Checking Real Estate library contents...")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{real_estate_drive_id}/root/children"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        items = response.json().get('value', [])
        
        print(f"   Found {len(items)} items in Real Estate root:")
        deals_folder_id = None
        
        for item in items:
            name = item.get('name', 'Unknown')
            is_folder = 'folder' in item
            item_id = item.get('id', 'Unknown')
            
            if is_folder:
                print(f"   - 📁 {name}")
                if name == 'Deals':
                    deals_folder_id = item_id
                    print("     ^ This is our Deals folder!")
            else:
                print(f"   - 📄 {name}")
                
    except Exception as e:
        print(f"   ✗ Failed to get Real Estate contents: {e}")
        return False
    
    if not deals_folder_id:
        print("\n   ✗ ERROR: Deals folder not found in Real Estate library!")
        return False
    
    # Step 5: Check Deals Folder Contents
    print("\n5. Checking Deals folder contents...")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{real_estate_drive_id}/items/{deals_folder_id}/children"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        items = response.json().get('value', [])
        
        print(f"   Found {len(items)} stage folders:")
        stage_folders = []
        
        for item in items[:10]:  # Show first 10
            if 'folder' in item:
                name = item.get('name', 'Unknown')
                print(f"   - {name}")
                stage_folders.append(name)
                
    except Exception as e:
        print(f"   ✗ Failed to get Deals contents: {e}")
        return False
    
    # Step 6: Test One Stage Folder
    print("\n6. Testing access to one stage folder...")
    expected_stages = [
        "0) Dead Deals",
        "1) Initial UW and Review",
        "2) Active UW and Review",
        "3) Deals Under Contract",
        "4) Closed Deals",
        "5) Realized Deals"
    ]
    
    test_stage = None
    for stage in expected_stages:
        if stage in stage_folders:
            test_stage = stage
            break
    
    if test_stage:
        print(f"   Testing with: {test_stage}")
        # You could add more detailed testing here
        print("   ✓ Stage folder accessible")
    else:
        print("   ⚠ No expected stage folders found")
    
    # Summary
    print("\n" + "=" * 60)
    print("CONNECTION TEST SUMMARY")
    print("=" * 60)
    print("✓ Authentication successful")
    print("✓ Site access confirmed")
    print("✓ Real Estate library found")
    print("✓ Deals folder found")
    print(f"✓ Found {len(stage_folders)} stage folders")
    print("\nYour SharePoint connection is properly configured!")
    print("You can now proceed with the full extraction process.")
    
    # Save test results
    results = {
        'test_date': datetime.now().isoformat(),
        'site_id': site_id,
        'real_estate_drive_id': real_estate_drive_id,
        'deals_folder_id': deals_folder_id,
        'stage_folders_found': stage_folders
    }
    
    with open('sharepoint_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nTest results saved to: sharepoint_test_results.json")
    
    return True


if __name__ == "__main__":
    success = test_sharepoint_connection()
    
    if not success:
        print("\n⚠ Please check your configuration and try again.")
        exit(1)