import os
import regex

# Matches full emoji grapheme clusters (safe for ZWJ, skin tones, flags, etc.)
emoji_pattern = regex.compile(r'\X', flags=regex.UNICODE)

def is_emoji(s):
    return emoji_pattern.fullmatch(s) and any(char in emoji_chars for char in s)

# Load known emoji characters safely
import emoji
emoji_chars = set(emoji.EMOJI_DATA.keys())

def remove_emojis(text):
    return ''.join(char for char in emoji_pattern.findall(text) if not is_emoji(char))

def remove_emojis_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = remove_emojis(content)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)

def walk_and_clean(directory):
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".py"):
                full_path = os.path.join(root, filename)
                remove_emojis_from_file(full_path)
                print(f"Cleaned: {full_path}")

# Usage: run in root of your project
walk_and_clean(".")
