import fractions
import time
import unicodedata

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re
import math
from nltk.corpus import words


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.CapitalTerms = {}

    def CheckIfNumber(self, term):
        ModifiedNumber = ""
        numOfDigits = 0
        for digit in term:
            if not digit.isnumeric() and digit is not ',' and digit is not '.' and digit is not '%' and digit is not '$' or digit is '/' or digit is '-':
                return None
            if digit is ',':
                continue
            if digit.isnumeric():
                numOfDigits += 1
            ModifiedNumber += digit
        return ModifiedNumber if numOfDigits > 0 else None

    ##Should recognize: Terms,Tags,Hashtags.
    def parse_sentence(self, text, term_dict, isRetweet=False):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        index = 0
        delimiters = "[ \n]"
        terms = re.split(delimiters, text)
        while index < len(terms):
            # Parse expression with dashes
            if '-' in terms[index]:
                splitedByDash = re.split("[-]", terms[index])
                for word in splitedByDash:
                    lower = word.lower()
                    if lower in words.words():
                        self.SaveTerm(lower, term_dict)
                if terms[index][0].islower():
                    self.SaveTerm(terms[index], term_dict)
            # Found URL in text - already parsed, continue
            if terms[index].__eq__('') or terms[index][:13].__eq__("https://t.co/"):
                index += 1
                continue
            # Parse as expression
            if terms[index][0].isupper():
                index = self.parseCapitalLetterWord('', terms, index, term_dict)
                continue
            # Parse as number
            ModifiedNumber = self.CheckIfNumber(terms[index])
            if ModifiedNumber is not None:
                NextTerm = None if (index == len(terms) - 1) else terms[index + 1]
                if (ModifiedNumber == ''):
                    print("here")
                if self.parseNumber(ModifiedNumber, NextTerm, term_dict):  # If function returns "True" - next term was related.
                    index += 2
                else:
                    index += 1
                continue
            # Parse as Tag
            elif terms[index][0].__eq__('@'):
                self.parseTag(terms[index], term_dict)
                index += 1
                continue
            # Parse as HashTag
            elif terms[index][0].__eq__('#'):
                self.parseHashTag(terms[index][1:], term_dict)
                index += 1
                continue
            # Save term as is
            self.SaveTerm(terms[index], term_dict)
            index += 1

        text_tokens = word_tokenize(text)
        text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        # self.parseURL(text, term_dict)
        return text_tokens_without_stopwords

    def checkFraction(self, fraction):
        if fraction is None:
            return False
        values = fraction.split('/')
        return len(values) == 2 and all(i.isdigit() for i in values)

    def SetSymbol(self, allNumeric, nextTerm, splited):
        Symbol = 'K' if (allNumeric and (3 < len(splited[0]) < 7 or nextTerm is 'Thousand')) else \
            'M' if (allNumeric and 6 < len(splited[0]) < 10 or nextTerm is 'Million') else \
                'B' if (allNumeric and len(splited[0]) > 9 or nextTerm is 'Billion') else \
                    '%' if (nextTerm is 'percent' or nextTerm is 'percentage'
                            or ((len(splited[0]) > 0 and splited[0][-1] is '%') or (len(splited) > 1 and len(splited[1]) > 0 and splited[1][-1] is '%'))) else \
                        (' ' + nextTerm) if self.checkFraction(nextTerm) else \
                            '$' if (len(splited[0]) > 0 and splited[0][-1] is '$' or (len(splited) > 1 and len(splited[1]) > 0 and splited[1][-1] is '$')
                                    or nextTerm is 'dollars' or nextTerm is 'bucks') else ''
        return Symbol

    def SetDivisor(self, allNumeric, splited):
        Divisor = 1 if ((not allNumeric) or len(splited[0]) < 4) else 1000 if allNumeric and 3 < len(
            splited[0]) < 7 else \
            1000000 if allNumeric and 6 < len(splited[0]) < 10 else 1000000000
        return Divisor

    def SetRemainder(self, splited, Percent_Dollar):
        Remainder = 0 if (len(splited) is 1 or splited[1] is '%' or splited[1] is '$') else \
            float(splited[1][:-1][:3]) / min(1000, math.pow(10, len(splited[1][:-1]))) if Percent_Dollar else \
                float(splited[1][:3]) / math.pow(10, len(splited[1]))
        return Remainder

    def SetNextTermWasUsed(self, nextTerm):
        return nextTerm is 'Thousand' or nextTerm is 'Million' or nextTerm is 'Billion' \
               or nextTerm is 'percent' or nextTerm is 'percentage' or nextTerm is 'bucks' \
               or nextTerm is 'dollars'

    def parseNumber(self, number, nextTerm, term_dict):
        nextTermWasUsed = self.SetNextTermWasUsed(nextTerm)
        splited = re.split('[./]', number)
        allNumeric = splited[0].isnumeric() and (not len(splited) > 1 or splited[1].isnumeric())
        Symbol = self.SetSymbol(allNumeric, nextTerm, splited)
        Divisor = self.SetDivisor(allNumeric, splited)
        if len(splited[0]) > 0 and (splited[0][-1] is '%' or splited[0][-1] is '$'):
            splited.append(splited[0][-1])
            splited[0] = splited[0][:-1]
        Percent_Dollar = True if (len(splited) > 1 and len(splited[1]) > 0 and splited[1][-1]) is '%' or (
                len(splited) > 1 and len(splited[1]) > 0 and splited[1][-1]) is '$' else False
        if nextTermWasUsed or Symbol is '%' or Symbol is '$' or self.checkFraction(nextTerm):
            Remainder = self.SetRemainder(splited, Percent_Dollar)
            number = str((splited[0])) + (("{:.4f}".format(Remainder)[:-1])[1:] if not Remainder == 0 else "") + Symbol
        else:
            splited[0] = unicodedata.numeric(splited[0][-1])
            fSplited = float(splited[0]) / Divisor
            number = "{:.4f}".format(fSplited)[:-1] + Symbol
        self.SaveTerm(number, term_dict)
        return nextTermWasUsed

    def parseCapitalLetterWord(self, text, terms, index, term_dict):
        if index >= len(terms) or len(terms[index]) == 0 or not terms[index][0].isalpha() or terms[index][0].islower():
            return index

        self.SaveCapital(terms[index], term_dict)

        recursiveText = terms[index]
        if text != '':
            recursiveText = text + ' ' + terms[index]
            self.SaveCapital(recursiveText, term_dict)

        # if index+1 < len(terms) and terms[index+1][0].isupper():
        index = self.parseCapitalLetterWord(recursiveText, terms, index + 1, term_dict)

        return index

    def SaveCapital(self, term, term_dict):
        lowerText = term.lower()
        if lowerText in term_dict:
            term_dict[lowerText] += 1
            return
        upperText = term.upper()
        self.SaveTerm(upperText, term_dict)

    def SaveTerm(self, term, term_dict):
        term = term.replace('?', '').replace('.', '').replace('!', '')
        if term in term_dict:
            term_dict[term] += 1
        else:
            term_dict[term] = 1

    def parseURL(self, text, term_dict):
        parsed = re.split('"', text)
        if len(parsed) > 3:
            to_be_parsed = parsed[3]
            splited = re.split("[:/?=&+]", to_be_parsed)
            self.SaveTerm(splited[0], term_dict)
            if splited[3][:3] == 'www':
                self.SaveTerm(splited[3][:3], term_dict)
                self.SaveTerm(splited[3][4:], term_dict)
            else:
                self.SaveTerm(splited[3], term_dict)
            for term in splited[4:]:
                self.parse_sentence(term, term_dict, False)

    def parseHashTag(self, term, term_dict):
        splitedByUnderScore = re.split('[_]', term)
        result = '#'
        for term in splitedByUnderScore:
            result += term.lower()
            self.parse_sentence(term, term_dict)
        self.SaveTerm(result, term_dict)

    def parseTag(self, term, term_dict):
        if term[-1] == ':':
            term = term[:-1]
        self.SaveTerm(term, term_dict)

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]  # This tweet ID.
        tweet_date = doc_as_list[1]  # This tweet Date.
        full_text = doc_as_list[2]  # Tweet's full text. If it's a re-tweet, start with 'RT @username_being_re-tweeted:' and the the 'pure' text.
        url = doc_as_list[3]  # If tweet contains urls, this list contains them. If user re-tweeted from outside source, it's url should be here.
        url_indices = doc_as_list[4]  # if tweet contains urls, this list contains their urls.
        retweet_text = doc_as_list[5]  # If this tweet is a re-tweet, this is the originals 'pure' text.
        retweet_url = doc_as_list[6]  # If this tweet is a re-tweet, this is the original's address (url).
        retweet_url_indices = doc_as_list[7]  # re-tweet indices
        quote_text = doc_as_list[8]  # If this is re-tweet, and the original tweet is a re-tweet, this is the original's tweet full text.
        quote_url = doc_as_list[9]  # If this is re-tweet, and the original tweet is a re-tweet, this is the original's address (url).
        quote_url_indices = doc_as_list[10]  # If this is re-tweet, and the original tweet is a re-tweet, this is the original's address (url) indices.
        retweet_quoted_text = doc_as_list[11]
        retweet_quoted_urls = doc_as_list[12]
        retweet_quoted_url_indices = doc_as_list[13]
        print(tweet_id)
        if tweet_id == "1280921204594036737":
            print("א")
        term_dict = {}  # Number of appearances of term per document.
        if url != '{}':
            self.parseURL(url, term_dict)
        if retweet_url != None:
            self.parseURL(retweet_url, term_dict)
        tokenized_text = self.parse_sentence(full_text, term_dict)  # All tokens in document

        doc_length = len(tokenized_text)  # after text operations.

        # for term in tokenized_text:
        #     if term not in term_dict.keys():
        #         term_dict[term] = 1
        #     else:
        #         term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

