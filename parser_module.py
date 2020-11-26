import fractions
import time
import unicodedata

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from numpy import unicode
from numpy.compat import basestring

from document import Document
import re
import math
from nltk.corpus import words


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')
        extra_stop_words = ['i\'ll', 'i\'d', 'i\'m', 'i\'ve', 'http', 'https', 'www']
        self.stop_words = self.stop_words + extra_stop_words
        self.FirstCharDict = {}
        self.LenDict = {}
        self.Doc_ID = 0

    def update(self):
        pass

    def CheckIfNumber(self, term):
        no_comas = term.replace(',', '')  # 2/3
        no_symbols = no_comas.replace('%', '').replace('$', '').replace('.', '')  # no_symbols = 453231.432
        if no_symbols.isnumeric() and (no_symbols == no_comas[:-1] or no_symbols == no_comas[1:]):
            return no_comas
        else:
            return None
        """
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
        """

    def clearNonASCII(self, s):
        string_encode = s.encode("ascii", "ignore")
        string_decode = string_encode.decode()
        return string_decode

    def cleanEdgeChars(self, term):
        stop = False
        while not stop:
            if len(term) < 2:
                term = re.sub(r"\.|,|;|'|\\|\"|\'|'*'|:|\)|\(|\r|\n|~|\+|{|}|=|^|&|_|\[|\]", '', term)
                return term
            temp = term
            term = re.sub(r"|\,|;|'|\\|\"|\'|'*'|:|\)|\(|\r|\n|~|\+|{|}|=|^|&|_|\[|\]", '', term[0]) + term[
                                                                                                       1:-1] + re.sub(
                r"\.|,|;|'|\\|\"|\'|'*'|:|\)|\(|\r|\n|~|\+|{|}|=|^|&|_|\[|\]", '', term[-1])
            if term == temp:
                stop = True
        return term

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
        # Snip three dots (...) from end of unfinished sentence.
        if terms[0] == 'RT':
            index = 1
        while index < len(terms):
            # Check for the empty string or URL - shouldn't be parsed.
            # if 'ऐ' in terms[index]:
            #     print('here')
            if terms[index].__eq__('') or terms[index][:13].__eq__("https://t.co/") or any(
                    unicodedata.category(char) == 'Lo' for char in terms[index]):
                index += 1
                continue
            if not unicodedata.category(terms[index][-1]) == 'No':
                terms[index] = self.clearNonASCII(terms[index])
            # If last char of term is not relevant than remove it.
            terms[index] = self.cleanEdgeChars(terms[index])
            if terms[index] is None or terms[index] == '':
                index += 1
                continue
            # Check for stop word - continue and don't add it.
            if terms[index].lower() in self.stop_words:
                index += 1
                continue
            # Parse as expression or entity
            if terms[index][0].isupper() and terms[index][0].isalpha():
                index = self.parseCapitalLetterWord('', terms, index, term_dict)
                continue
            # Parse as number
            ModifiedNumber = self.CheckIfNumber(terms[index])
            if ModifiedNumber is not None:
                NextTerm = None if (index == len(terms) - 1) or terms[index + 1] == '' else terms[index + 1]
                if self.parseNumber(ModifiedNumber, NextTerm,
                                    term_dict):  # If function returns "True" - next term was related.
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

        # text_tokens = word_tokenize(text)
        # text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        # self.parseURL(text, term_dict)
        return term_dict.keys()

    def checkFraction(self, fraction):
        if fraction is None:
            return False
        values = fraction.split('/')
        return len(values) == 2 and all(i.isdigit() for i in values)

    def SetSizeSymbol(self, nextTerm, number):
        Symbol = 'K' if (3 < len(number) < 7 or nextTerm == 'thousand') else \
            'M' if (6 < len(number) < 10 or nextTerm == 'million') else \
                'B' if (len(number) > 9 or nextTerm == 'billion') \
                    else ''
        return Symbol

    def SetValueSymbol(self, nextTerm, splited):
        HasSecondElement = len(splited) > 1
        SecondElementNonEmpty = HasSecondElement and len(splited[1]) > 0
        valueSymbol = '%' if splited[0][0] == '%' or splited[0][-1] == '%' or (
                SecondElementNonEmpty and splited[1][-1] == '%') \
                             or nextTerm == 'percent' or nextTerm == 'percentage' \
            else '$' if splited[0][0] == '$' or splited[0][-1] == '$' or (
                SecondElementNonEmpty and splited[1][-1] == '$') \
                        or nextTerm == 'buck' or nextTerm == 'dollar' \
            else ''
        return valueSymbol

    def SetDivisor(self, number):
        Divisor = 1 if (len(number) < 4) else 1000 if 3 < len(number) < 7 else \
            1000000 if 6 < len(number) < 10 else 1000000000
        return Divisor

    def SetRemainder(self, splited, sizeSymbol):
        Remainder = '0'
        # In case of large number
        if sizeSymbol != '' and len(splited[0]) > 3:
            Module = 3 if len(splited[0]) % 3 == 0 else len(splited[0]) % 3
            Remainder = '0.' + splited[0][Module:Module + 3]
            return Remainder
        numOfDigits = 0
        if len(splited) > 1:
            numOfDigits = min(3, len(splited[1]))
        # Has digits after floating point.
        if numOfDigits != 0:
            Remainder = '0.' + splited[1][:numOfDigits + 1]
        return Remainder

    def SetNextTermWasUsed(self, nextTerm):
        return nextTerm == 'thousand' or nextTerm == 'million' or nextTerm == 'billion' \
               or nextTerm == 'percent' or nextTerm == 'percentage' or nextTerm == 'buck' \
               or nextTerm == 'dollar' or False

    def checkForUnicode(self, number, fraction, term_dict):
        fraction_is_not_unicode = True
        number_is_not_unicode = True
        number_until_last_not_unicode = True
        ################ 5,5    1/2, '',  20 1/2, '' 20
        # check each path - if unicode or not
        for i in range(0, len(number)):
            if unicodedata.category(number[i]) != 'Nd':
                number_is_not_unicode = False
                if i != len(number) - 1:
                    number_until_last_not_unicode = False
                break
        #####################################
        if not number_is_not_unicode:
            if number_until_last_not_unicode and len(number) != 1:  # numeric and fraction in number
                asFraction = unicodedata.numeric(number[-1])
                corrected_number = str(float(number[:-1]) + float(asFraction))
                splitedFraction = re.split("[.]", corrected_number)
                return splitedFraction[0], splitedFraction[1], False
            else:  # single fraction
                result = 0
                for i in range(len(number), 0):
                    result += int(unicodedata.numeric(number[i])) * math.pow(10, i)
                self.SaveTerm(str(result), term_dict)
                return str(result), fraction, True
        else:
            return number, fraction, False

        """
        for digit in number:
            try:
                digit = str(int(digit))
            except:
                pass
            if not 47 < int(ord(digit)) < 58 : #Not a regular digit.
                try:
                    splited =re.split("[.]",str(int(number[:-1])+float(unicodedata.numeric(number[-1]))))
                    corrected_number = splited[0]
                    remainder=splited[1]
                except:
                    try:
                        corrected_number = int(unicodedata.numeric(number))
                        remainder = fraction
                    except:
                        self.SaveTerm(number, term_dict)
                        return number,fraction, True
                return str(corrected_number), str(remainder), False
        return number,fraction, False
        """

    def parseNumber(self, number, nextTerm, term_dict):
        # print("begin parse number: ", number)
        if nextTerm is not None:
            l_nextTerm = nextTerm.lower()
            l_nextTerm = l_nextTerm[:-1] if l_nextTerm[-1] is 's' else l_nextTerm
            nextTermWasUsed = self.SetNextTermWasUsed(l_nextTerm)
        else:
            nextTermWasUsed = False
            l_nextTerm = None
        splited = re.split('[./]', number)
        # Set value symbol: $ or %
        if splited[0] == '':  # In case of empty element in first index.
            splited[0] = '0'
        valueSymbol = self.SetValueSymbol(l_nextTerm, splited)
        # Clear value symbol from string
        if valueSymbol != '':
            splited[0] = splited[0].replace(valueSymbol, '')
            if splited[0] == '':  # In case of empty element after replacement.
                splited[0] = '0'
            if len(splited) > 1:
                splited[1] = splited[1].replace(valueSymbol, '')
        if len(splited) == 1:
            splited.append('')
        splited[0], splited[1], Saved = self.checkForUnicode(splited[0], splited[1], term_dict)
        if Saved:
            return nextTermWasUsed
        sizeSymbol = self.SetSizeSymbol(l_nextTerm, splited[0])  # Set the symbol if needed (K/M/B)
        Divisor = self.SetDivisor(splited[0])  # Set the divisor if needed (1/1,000/100,000/1,000,000)
        Remainder = self.SetRemainder(splited, sizeSymbol)  # Set the remainder as string
        valRemainder = float(Remainder) if float(Remainder) != 0 else 0  # Get remainder's numeric value
        try:
            int(splited[0])
        except:
            # if len(splited[0]) > 1:
            #     print("len splited[0]: ", len(splited[0]))
            #     print(isinstance(splited[0], unicode))
            try:
                corrected_number = str(float(splited[0][:-1]) + unicodedata.numeric(splited[0][-1]))
            except:
                try:
                    corrected_number = str(unicodedata.numeric(splited[0]))
                except:
                    self.SaveTerm(splited[0], term_dict)
                    return nextTermWasUsed
            # else:
            #     number = str(unicodedata.numeric(splited[0]))
            # print("Before recursive call")
            # print(corrected_number)
            return self.parseNumber(corrected_number, nextTerm, term_dict)
        # print("splited[0]: ", splited[0])
        number = str(int(int(splited[0]) / Divisor) + valRemainder) + sizeSymbol + valueSymbol  # Connect all as string
        self.SaveTerm(number, term_dict)
        return nextTermWasUsed

        """
        nextTermWasUsed = self.SetNextTermWasUsed(nextTerm) # Check if next term is relevant to parsing.
        splited = re.split('[./]', number)
        allNumeric = splited[0].isnumeric() and (not len(splited) > 1 or splited[1].isnumeric()) #Allnumeric - all elements in splited are numeric.
        Symbol = self.SetSymbol(allNumeric, nextTerm, splited) # Set the symbol if needed (K/M/B/%/$)
        Divisor = self.SetDivisor(allNumeric, splited) # Set the divisor if needed (1/1,000/100,000/1,000,000)
        if len(splited[0]) > 0 and (splited[0][-1] is '%' or splited[0][-1] is '$'): # floating point & symbol correction.
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
    """

    def parseCapitalLetterWord(self, text, terms, index, term_dict):
        if index >= len(terms) or len(terms[index]) == 0 or not terms[index][0].isalpha() or terms[index][0].islower():
            return index

        terms[index] = self.cleanEdgeChars(terms[index])
        if terms[index] is None:
            return index
        self.SaveCapital(terms[index], term_dict)
        recursiveText = terms[index]
        if text != '':
            recursiveText = text + ' ' + recursiveText
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

    def SaveTerm(self, term, term_dict):  # add boolean if last word in sen.
        term = term.replace('?', '').replace('!', '')
        if term == '':
            return
        if term == '' or term.lower() in self.stop_words:
            return
        if term in term_dict:
            term_dict[term] += 1
        else:
            term_dict[term] = 1

    def parseURL(self, text, term_dict):
        parsed = re.split('"', text)
        if len(parsed) > 3:
            to_be_parsed = parsed[3]
            splited = re.split("[:/?=&+-]", to_be_parsed)
            self.SaveTerm(splited[0], term_dict)
            if splited[3][:3] == 'www':
                self.SaveTerm(splited[3][:3], term_dict)
                self.SaveTerm(splited[3][4:], term_dict)
            else:
                self.SaveTerm(splited[3], term_dict)
            for term in splited[4:]:
                self.SaveTerm(term, term_dict)

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
        full_text = doc_as_list[
            2]  # Tweet's full text. If it's a re-tweet, start with 'RT @username_being_re-tweeted:' and the the 'pure' text.
        url = doc_as_list[
            3]  # If tweet contains urls, this list contains them. If user re-tweeted from outside source, it's url should be here.
        url_indices = doc_as_list[4]  # if tweet contains urls, this list contains their urls.
        retweet_text = doc_as_list[5]  # If this tweet is a re-tweet, this is the originals 'pure' text.
        retweet_url = doc_as_list[6]  # If this tweet is a re-tweet, this is the original's address (url).
        retweet_url_indices = doc_as_list[7]  # re-tweet indices
        quote_text = doc_as_list[
            8]  # If this is re-tweet, and the original tweet is a re-tweet, this is the original's tweet full text.
        quote_url = doc_as_list[
            9]  # If this is re-tweet, and the original tweet is a re-tweet, this is the original's address (url).
        quote_url_indices = doc_as_list[
            10]  # If this is re-tweet, and the original tweet is a re-tweet, this is the original's address (url) indices.
        retweet_quoted_text = doc_as_list[11]
        retweet_quoted_urls = doc_as_list[12]
        retweet_quoted_url_indices = doc_as_list[13]
        # print(tweet_id)
        # if tweet_id == '1283747919804329984':
        #     print('asd')
        term_dict = {}  # Number of appearances of term per document.
        if url != '{}':
            self.parseURL(url, term_dict)
        if retweet_url is not None:
            self.parseURL(retweet_url, term_dict)
        # if retweet_quoted_urls is not None:
        #     self.parseURL(retweet_quoted_urls, term_dict)
        #start = time.time()
        #full_text = "#MIFF 68½ wanders the world, offering a digital feast in lockdown @MIFFofficial https://theage.com.au/culture/movies/miff-68-wanders-the-world-offering-a-digital-feast-in-lockdown-20200715-p55c95.html via @theage"
        tokenized_text = self.parse_sentence(full_text, term_dict)  # All tokens in document
        # print(time.time() -start)
        doc_length = len(tokenized_text)  # after text operations.

        document = Document(self.Doc_ID, tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        self.Doc_ID += 1
        return document
