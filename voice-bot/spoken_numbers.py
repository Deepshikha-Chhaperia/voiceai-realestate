"""Lossless, idempotent spoken rendering. No model calls or stored-value changes."""
import re
from decimal import Decimal

_SMALL = 'zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen'.split()
_TENS = ['', '', 'twenty', 'thirty', 'forty', 'fifty', 'sixty', 'seventy', 'eighty', 'ninety']


def cardinal(n):
    n = int(n)
    if n < 20:
        return _SMALL[n]
    if n < 100:
        return _TENS[n // 10] + (' ' + _SMALL[n % 10] if n % 10 else '')
    for base, unit in ((10000000, 'crore'), (100000, 'lakh'), (1000, 'thousand'), (100, 'hundred')):
        if n >= base:
            return cardinal(n // base) + ' ' + unit + (' ' + cardinal(n % base) if n % base else '')


def decimal_words(value):
    parts = value.split('.')
    return cardinal(parts[0]) + (' point ' + ' '.join(_SMALL[int(d)] for d in parts[1]) if len(parts) == 2 else '')


def area_words(value):
    n = int(value.replace(',', ''))
    if 1100 <= n <= 2500:
        return cardinal(n // 100) + ' hundred' + (' ' + cardinal(n % 100) if n % 100 else '')
    return cardinal(n)


def spoken_numbers(text):
    # Never alter opaque addresses/identifiers or phone-like long digit strings.
    saved = []
    def protect(m):
        saved.append(m.group())
        return '\ue000' + chr(0xe100 + len(saved) - 1) + '\ue001'
    text = re.sub(r'https?://\S+|\b[\w.+-]+@[\w.-]+\.[A-Za-z]+|\b\d{4}-\d{2}-\d{2}\b|\b[A-Za-z_][\w-]*\d[\w-]*\b|\b\d{3}[- ]\d{3}[- ]\d{4}\b|\b\d{8,}\b|\+\d[\d -]{8,}\d|\b\d{5}(?:-\d{4})?\b|\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b', protect, text)
    # Ranges must be made explicit BEFORE converting digits to words.
    text = re.sub(r'(?<=\d)\s*[-–]\s*(?=\d)', ' to ', text)
    text = text.replace('twenty-one hundred', 'twenty one hundred').replace('twenty-two hundred', 'twenty two hundred')
    # Price values stay exact. Hindi utterances use natural crore/lakh speech where lossless.
    hindi = bool(re.search(r'[\u0900-\u097f]|\b(?:mein|wale|hai|baje|aap|kal)\b', text, re.I))
    hindi_lakh = {45:'paintalis',65:'painsath',70:'sattar',80:'assi',85:'pachasi',95:'pachanave'}
    def money(m):
        value, unit = m.group(1), m.group(2).lower()
        if hindi and unit.startswith(('cr', 'crore')):
            amount = Decimal(value)
            crores = int(amount)
            lakh = (amount - crores) * 100
            if lakh == int(lakh) and int(lakh) in hindi_lakh and crores == 1:
                return 'ek crore ' + hindi_lakh[int(lakh)] + ' lakh'
        return decimal_words(value) + (' crore' if unit.startswith('cr') else ' lakh')
    def money_range(m):
        return money(re.match(r'(.*) (.*)', m[1] + ' ' + m[4])) + ' ' + ('se' if hindi else 'to') + ' ' + money(re.match(r'(.*) (.*)', m[3] + ' ' + m[4]))
    text = re.sub(r'\b(\d+(?:\.\d+)?)\s+(to|se)\s+(\d+(?:\.\d+)?)\s*(crores?|cr|lakhs?|lac)\b', money_range, text, flags=re.I)
    text = re.sub(r'₹\s*(?=\d+(?:\.\d+)?\s*(?:crores?|cr|lakhs?|lac)\b)', '', text, flags=re.I)
    text = re.sub(r'₹\s*(\d+(?:\.\d+)?)', lambda m: decimal_words(m[1]) + ' rupees', text)
    text = re.sub(r'\b(\d+(?:\.\d+)?)\s*(crores?|cr|lakhs?|lac)\b', money, text, flags=re.I)
    text = re.sub(r'\b(\d{3,5}(?:,\d{3})?)\b(?=\s*(?:to|se)\s*\d{3,5}\s*(?:square feet|feet|sq\.?\s*ft\.?)|\s*(?:square feet|feet|sq\.?\s*ft\.?))', lambda m: area_words(m[0]), text, flags=re.I)
    # Common campaign areas in ranges after the first replacement.
    text = re.sub(r'\b(\d{3,5})\b(?=\s*(?:square feet|feet|sq\.?\s*ft\.?))', lambda m: area_words(m[0]), text, flags=re.I)
    def clock(m):
        hour, minutes, period = int(m[1]), int(m[2] or 0), m[3].lower()
        if not 1 <= hour <= 12 or not 0 <= minutes <= 59:
            return m[0]
        spoken = cardinal(hour) + (' ' + ('oh ' if minutes < 10 else '') + cardinal(minutes) if minutes else '')
        return spoken + (' a m' if period == 'am' else ' p m')
    text = re.sub(r'\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b', clock, text, flags=re.I)
    text = re.sub(r'\b(\d{1,2}):(\d{2})\b', lambda m: cardinal(m[1]) + (" o'clock" if m[2] == '00' else ' ' + ('oh ' if int(m[2]) < 10 else '') + cardinal(m[2])) if int(m[1]) < 24 and int(m[2]) < 60 else m[0], text)
    text = re.sub(r'\b24/7\b', 'round the clock', text)
    text = re.sub(r'\b(20[2-9]\d)\b', lambda m: 'twenty ' + cardinal(int(m[1]) % 100), text)
    text = re.sub(r'\b(\d+(?:\.\d+)?)\b', lambda m: decimal_words(m[1]) if len(m[1].split('.')[0]) <= 4 else m[0], text)
    text = re.sub(r'\bbhk\b', 'B H K', text, flags=re.I)
    text = re.sub(r'(?<=\w)\s*%',' percent',text)
    for i, value in enumerate(saved):
        text = text.replace('\ue000' + chr(0xe100 + i) + '\ue001', value)
    return text
