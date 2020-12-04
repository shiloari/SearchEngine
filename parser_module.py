import unicodedata
from nltk.corpus import stopwords
import stemmer
from document import Document
import re
import math

class Parse:

    def __init__(self, Stemming = False):
        self.stop_words = stopwords.words('english')
        extra_stop_words = ['i\'ll', 'i\'d', 'i\'m', 'i\'ve'] #expand the stopwords
        self.stop_words = self.stop_words + extra_stop_words
        self.Doc_ID = 0
        self.Stemmer = None
        if Stemming:
            self.Stemmer = stemmer.Stemmer()

    def isEntity(self, s):
        return ' ' in s and s.isupper() # Entity is more than one word and all capital (the word is after parsing)

    def CheckIfNumber(self, term):
        """
        :param term: term to check if number by the parser rules.
        :return: the number without comas if it is a number, else None
        """
        no_comas = term.replace(',', '')  # 2/3
        no_symbols = no_comas.replace('%', '').replace('$', '').replace('.', '')  # no_symbols = 453231.432
        if no_symbols.isnumeric() and (no_symbols == no_comas[:-1] or no_symbols == no_comas[1:]):
            return no_comas
        else:
            return None

    def clearNonASCII(self, s):
        """
        :param s: string to clean from all non-ascii chars.
        :return: the string cleaned up
        """
        string_encode = s.encode("ascii", "ignore")
        string_decode = string_encode.decode()
        return string_decode

    def cleanEdgeChars(self, term):
        """
        :param term: term to be cleaned from edges from all un-important chars.
        :return: the term cleaned up.
        """
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

    def parse_sentence(self, text, term_dict):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text to be parsed, and the document terms dictionary to be add to.
        :return: the document's terms dictionary
        """
        index = 0
        delimiters = "[ \n]"
        terms = re.split(delimiters, text)
        if terms[0] == 'RT':    #un important popular term
            index = 1
        while index < len(terms):
            # Check for the empty string or URL - shouldn't be or already parsed.
            if terms[index].__eq__('') or terms[index][:13].__eq__("https://t.co/") or any(
                    unicodedata.category(char) == 'Lo' for char in terms[index]):
                index += 1
                continue
            # UnicodeData can not be parsed in the Non-ASCII func.
            if not unicodedata.category(terms[index][-1]) == 'No':
                terms[index] = self.clearNonASCII(terms[index])
            # clear all non-important chars from string edges.
            terms[index] = self.cleanEdgeChars(terms[index])
            if terms[index] is None or terms[index] == '':
                index += 1
                continue
            # Check for stop word - continue and don't add it.
            if terms[index].lower() in self.stop_words:
                index += 1
                continue
            # Parse as expression or entity - recursive!
            if terms[index][0].isupper() and terms[index][0].isalpha():
                index = self.parseCapitalLetterWord('', terms, index, term_dict)
                continue
            # Parse as number, first check if pattern matches
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
        return term_dict


    def SetSizeSymbol(self, nextTerm, number):
        """
        :param nextTerm: next term in list
        :param number: current term in list - a number for sure
        :return: the symbol to attach to the number if there should be one.
        """
        Symbol = 'K' if (3 < len(number) < 7 or nextTerm == 'thousand') else \
            'M' if (6 < len(number) < 10 or nextTerm == 'million') else \
                'B' if (len(number) > 9 or nextTerm == 'billion') \
                    else ''
        return Symbol

    def SetValueSymbol(self, nextTerm, splited):
        """
        :param nextTerm: the next term in list
        :param splited: the current term splited by floating point
        :return: % or $ if should be attached to number
        """
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
        """
        :param number: the term
        :return: if number should be divided to match the parsing pattern
        """
        Divisor = 1 if (len(number) < 4) else 1000 if 3 < len(number) < 7 else \
            1000000 if 6 < len(number) < 10 else 1000000000
        return Divisor

    def SetRemainder(self, splited, sizeSymbol):
        """
        :param splited: term splited
        :param sizeSymbol: the size of term
        :return: if there is a remainder, return it.
        """
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
        """
        :param nextTerm: next term in sentence
        :return: if next term matches any number patterns, return true.
        """
        return nextTerm == 'thousand' or nextTerm == 'million' or nextTerm == 'billion' \
               or nextTerm == 'percent' or nextTerm == 'percentage' or nextTerm == 'buck' \
               or nextTerm == 'dollar' or False

    def checkForUnicode(self, number, fraction, term_dict):
        """
        :param number: the whole value of number.
        :param fraction: the fraction.
        :param term_dict: the dictionary of doc.
        :return: if was unicode, parse it to number and return all parts of it
        """
        number_is_not_unicode = True
        number_until_last_not_unicode = True
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

    def parseNumber(self, number, nextTerm, term_dict):
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
            try:
                corrected_number = str(float(splited[0][:-1]) + unicodedata.numeric(splited[0][-1]))
            except:
                try:
                    corrected_number = str(unicodedata.numeric(splited[0]))
                except:
                    self.SaveTerm(splited[0], term_dict)
                    return nextTermWasUsed
            return self.parseNumber(corrected_number, nextTerm, term_dict)
        number = str(int(int(splited[0]) / Divisor) + valRemainder) + sizeSymbol + valueSymbol  # Connect all as string
        self.SaveTerm(number, term_dict)
        return nextTermWasUsed

    def parseCapitalLetterWord(self, text, terms, index, term_dict):
        """
        :param text: the current text from recursive call
        :param terms: all terms.
        :param index: the current index in terms list.
        :param term_dict: the dictionary.
        :return: return the phrase.
        """
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
        term = term.replace('?', '').replace('!', '')
        l_term = term.lower()
        u_term = term.upper()
        if term == '':
            return
        if term == '' or l_term in self.stop_words:
            return
        wasCapital = (term.isalpha() and term[0] == u_term[0])
        if self.Stemmer is not None:
            stemmed = self.Stemmer.stem_term(term)
            term = stemmed.upper() if wasCapital else stemmed.lower()
        else:
            if not wasCapital:
                term = l_term
            else:
                term = u_term
        if term in term_dict:
            term_dict[term] += 1
        elif u_term in term_dict and term == l_term:
            term_dict[term] = term_dict[u_term] + 1
            term_dict.pop(u_term)
        else:
            term_dict[term] = 1

    def parseURL(self, text, term_dict):
        url_stop_words = ['status', 'web', 'i', 'p'] #common url words with no meaning.
        parsed = re.split('"', text)
        if len(parsed) > 3:
            to_be_parsed = parsed[3]
            splited = re.split("[:/?=&+-]", to_be_parsed)
            if splited[3][:3] == 'www':
                self.SaveTerm(splited[3][4:], term_dict)
            else:
                self.SaveTerm(splited[3], term_dict)
            for term in splited[4:]:
                if term not in url_stop_words:
                    self.SaveTerm(term, term_dict)

    def parseHashTag(self, term, term_dict):
        term = term.replace('#','') #clear hashtag from #.
        if term == '':
            return
        #parse by underscore
        splitedByUnderScore = re.split('[_]', term)
        result = '#'
        for term in splitedByUnderScore:
            splitedWords = re.split('(?=[A-Z])', term)
            for splitedWord in splitedWords:
                result += splitedWord.lower()
                self.parse_sentence(splitedWord, term_dict)
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
        term_dict = {}  # Number of appearances of term for this document.
        if url != '{}': #contains urls, parse them!
            self.parseURL(url, term_dict)
        if retweet_url is not None:
            self.parseURL(retweet_url, term_dict) #parse retweet urls
        tokenized_text = self.parse_sentence(full_text, term_dict)  # All tokens in document
        doc_length = len(tokenized_text.keys())  # after text operations.
        document = Document(self.Doc_ID, tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        self.Doc_ID += 1
        return document
