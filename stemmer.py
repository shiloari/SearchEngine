from nltk.stem import snowball
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize

class Stemmer:
    def __init__(self):
        self.stemmer = snowball.SnowballStemmer("english")
        self.porterStemmer = PorterStemmer()

    def stem_term(self, token):
        """
        This function stem a token
        :param token: string of a token
        :return: stemmed token
        """
        return self.porterStemmer.stem(token)
