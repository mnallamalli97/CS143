#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import sys
import json

__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:

    parsed_text = ''
    unigrams = ''
    bigrams = ''
    trigrams = ''

    #replace new lines and tab characters with a single space
    text = re.sub(r'\s+', ' ', text)

    #remove URLs, subreddits, replace with stuff
    text = re.sub(r'[\(]?http\S+[\)]?|][\)]', '', text)

    #remove all punctuation
    text = re.sub(r"[^a-zA-Z0-9.!?,;:'() -]*", '', text)

    
    

    #separate all external punctuation
    text = re.sub(r'\. ', ' . ', text)
    text = re.sub(r'! ', ' ! ', text)
    text = re.sub(r"\? ", ' ? ', text)
    text = re.sub(r', ', ' , ', text)
    text = re.sub(r'; ', ' ; ', text)
    text = re.sub(r': ', ' : ', text)
    text = re.sub(r'- ', ' ', text)
    text = re.sub(r' -', ' ', text)
    
    text = [token.lower() for token in text.split()]

    l_text = len(text)
    p = string.punctuation


    #this is its own list......
    text[l_text-1].split('.')



    # text.append()
    # print(text)
    bool_var = False


    gg = len(text[l_text-1])
    for i in range (0, gg):
    	if text[l_text-1][i] in ['.', ',', '!', '?', ';', ':']:
            #todo delete last char
            bool_var = True
            word = text[l_text-1][0:gg-1]
            last_char = text[l_text-1][gg-1]

    

    if bool_var == True:
        del text[-1]
        text.append(word)
        text.append(last_char)


    l_text = len(text)

 

    for i in range (l_text):
        parsed_text += text[i]
        if i != l_text - 1:
            parsed_text += ' '
        if text[i] not in p:
            unigrams += text[i]
            unigrams += ' '
    for i in range (l_text - 1):
        if text[i] not in p and text[i+1] not in p:
            bigrams += text[i] + '_' + text[i+1]
            bigrams += ' '
    for i in range (l_text - 2):
        if text[i] not in p and text[i+1] not in p and text[i+2] not in p:
            trigrams += text[i] + '_' + text[i+1] + '_' + text[i+2]
            trigrams += ' '
    if (unigrams.endswith(' ')):
        unigrams = unigrams[:-1]
    if (bigrams.endswith(' ')):
        bigrams = bigrams[:-1]
    if (trigrams.endswith(' ')):
        trigrams = trigrams[:-1]

    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    filename = sys.argv[1]

    with open(filename, "r") as json_data:
        for l in json_data:
            data = json.loads(l)
            print(sanitize(data['body']))
            break

    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.
