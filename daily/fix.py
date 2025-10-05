import json
import re
import os
import glob
from pathlib import Path


def is_actual_option(text):
    """
    Determine if text is an actual 'would you rather' option vs descriptive text
    """
    text_lower = text.lower().strip()

    # Patterns that indicate descriptive/introductory text rather than options
    descriptive_patterns = [
        r'^questions.*(for|are|way|to|can|will|help)',
        r'.*the following are.*',
        r'.*some .* questions.*',
        r'^these .* questions.*',
        r'.*brilliant way to.*',
        r'.*encouraged to.*',
        r'.*developing .* skills.*',
        r'.*transferred to.*',
        r'.*range from.*',
        r'.*just as much fun.*',
        r'.*intriguing and.*',
        r'.*determine if.*',
        r'.*including.*',
        r'.*intentionally.*',
        r'.*bound to have.*',
        r'.*classic game.*',
        r'.*created sets of.*',
        r'.*how well do you know.*',
        r'^questions ask players.*',
        r'.*equally challenging.*',
        r'.*simple format.*',
        r'.*often leads to.*',
        r'.*for all ages.*',
        r'.*great for getting started.*',
        r'.*works well in.*'
    ]

    # Check if text matches any descriptive patterns
    for pattern in descriptive_patterns:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return False

    # Real options are typically shorter and more direct
    word_count = len(text.split())
    if word_count > 25:  # Options are usually concise
        return False

    # Options usually start with action verbs or direct scenarios
    option_indicators = [
        r'^would you rather',
        r'^have a',
        r'^be able to',
        r'^live in',
        r'^work as',
        r'^travel to',
        r'^meet',
        r'^eat',
        r'^drink',
        r'^wear',
        r'^fight',
        r'^win',
        r'^lose',
        r'^gain',
        r'^never',
        r'^always'
    ]

    for indicator in option_indicators:
        if re.search(indicator, text_lower):
            return True

    # If it's short and doesn't match descriptive patterns, it's probably an option
    return word_count < 15


def extract_actual_options(text):
    """
    Try to extract actual options from descriptive text
    """
    # Look for patterns like "1. would you rather..." or numbered options
    option_patterns = [
        r'\d+\.\s*(would you rather\s*.*?(?:\?|\.|$))',
        r'would you rather\s*.*?\?',
        r'-?\s*(.*?\?)\s*-?',
    ]

    options = []
    for pattern in option_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]  # Take the first group if it's a tuple
            clean_option = match.strip()
            if len(clean_option.split()) < 20:  # Reasonable length for an option
                options.append(clean_option)

    return options


def fix_json_file(file_path):
    """
    Fix a JSON file containing would you rather questions
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Parse JSON
        data = json.loads(content)

        fixed_data = []
        removed_count = 0
        fixed_count = 0

        for item in data:
            option1 = item.get('option1', '')
            option2 = item.get('option2', '')

            # Check if both options are actual options
            option1_is_valid = is_actual_option(option1)
            option2_is_valid = is_actual_option(option2)

            if option1_is_valid and option2_is_valid:
                # Both options are valid, keep as is
                fixed_data.append(item)
            elif not option1_is_valid and not option2_is_valid:
                # Try to extract actual options from the text
                options1 = extract_actual_options(option1)
                options2 = extract_actual_options(option2)

                if len(options1) >= 2:
                    # Found multiple options in option1 text
                    item['option1'] = options1[0]
                    item['option2'] = options1[1] if len(options1) > 1 else options1[0]
                    fixed_data.append(item)
                    fixed_count += 1
                elif options1 and options2:
                    # Use extracted options
                    item['option1'] = options1[0]
                    item['option2'] = options2[0]
                    fixed_data.append(item)
                    fixed_count += 1
                else:
                    # Cannot extract valid options, remove this entry
                    removed_count += 1
                    print(f"Removed entry (cannot extract options): {option1[:50]}...")
            else:
                # One option is valid, one is not - try to fix the invalid one
                if not option1_is_valid:
                    options = extract_actual_options(option1)
                    if options:
                        item['option1'] = options[0]
                        fixed_count += 1
                    else:
                        removed_count += 1
                        print(f"Removed entry (invalid option1): {option1[:50]}...")
                        continue

                if not option2_is_valid:
                    options = extract_actual_options(option2)
                    if options:
                        item['option2'] = options[0]
                        fixed_count += 1
                    else:
                        removed_count += 1
                        print(f"Removed entry (invalid option2): {option2[:50]}...")
                        continue

                fixed_data.append(item)

        # Save fixed file
        output_path = file_path.replace('.json', '_fixed.json') if not file_path.endswith('_fixed.json') else file_path

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(fixed_data, f, indent=2, ensure_ascii=False)

        print(f"Fixed {file_path}:")
        print(f"  - Original entries: {len(data)}")
        print(f"  - Fixed entries: {len(fixed_data)}")
        print(f"  - Entries removed: {removed_count}")
        print(f"  - Entries fixed: {fixed_count}")
        print(f"  - Saved to: {output_path}")

        return fixed_data

    except json.JSONDecodeError as e:
        print(f"Error parsing {file_path}: {e}")
        return None
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None


def process_directory(directory_path):
    """
    Process all JSON files in a directory
    """
    json_files = glob.glob(os.path.join(directory_path, "*.json"))

    for json_file in json_files:
        if not json_file.endswith('_fixed.json'):  # Skip already fixed files
            print(f"\nProcessing: {json_file}")
            fix_json_file(json_file)


def preview_fixes(file_path):
    """
    Preview what changes will be made without actually modifying files
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        data = json.loads(content)

        print(f"Preview for {file_path}:")
        print("=" * 50)

        for i, item in enumerate(data[:10]):  # Preview first 10 entries
            option1 = item.get('option1', '')
            option2 = item.get('option2', '')

            option1_valid = is_actual_option(option1)
            option2_valid = is_actual_option(option2)

            print(f"Entry {i + 1}:")
            print(f"  Option1 valid: {option1_valid}")
            print(f"  Option1: {option1[:80]}...")
            print(f"  Option2 valid: {option2_valid}")
            print(f"  Option2: {option2[:80]}...")

            if not option1_valid or not option2_valid:
                extracted1 = extract_actual_options(option1)
                extracted2 = extract_actual_options(option2)
                print(f"  Extracted from option1: {extracted1}")
                print(f"  Extracted from option2: {extracted2}")

            print("-" * 30)

    except Exception as e:
        print(f"Error previewing {file_path}: {e}")


# Main execution
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = input("Enter file or directory path: ").strip()

    if os.path.isfile(target):
        # Preview first
        preview_fixes(target)

        # Ask for confirmation
        response = input("\nProceed with fixing? (y/n): ").lower()
        if response == 'y':
            fix_json_file(target)
    elif os.path.isdir(target):
        process_directory(target)
    else:
        print("Invalid path provided")