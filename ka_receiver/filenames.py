"""Validate an unchanged filename before using remote metadata as a disk path."""

import re


def original_filename(value: str) -> str:
    if (not value or value in ('.', '..') or value.endswith((' ', '.'))
            or any(ord(char) < 32 or char in '<>:"/\\|?*' for char in value)
            or re.fullmatch(r'(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])',
                            value.split('.')[0], re.IGNORECASE)):
        raise ValueError('A safe original filename without directory components is required')
    return value
