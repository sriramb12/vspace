import re
import unicodedata


WORD_PATTERN = re.compile("\w+", re.MULTILINE)


def normalize(txt, pattern=WORD_PATTERN, form="NFC"):
    """
    Unicode normalize txt to given form, convert to lowercase and
    extract patterns
    :param txt: str
    :param pattern: _sre.SRE_Pattern - compiled regular expression
    :form str: A valid Unicode form
    :return: str
    """
    return " ".join(pattern.findall(unicodedata.normalize(form, txt).lower()))
