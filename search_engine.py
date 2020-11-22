import time
import unicodedata

from numpy import unicode

from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import glob
from pathlib import Path
def run_engine(corpus_path, output_path, stemming):
    """

    :return:
    """
    number_of_documents = 0

    config = ConfigClass()
    r = ReadFile(corpus_path=corpus_path)
    p = Parse()
    indexer = Indexer(output_path)
    globList = []
    counter = 0
    counter2 = 0
    counter3 = 0
    for path in Path(corpus_path).rglob('*.parquet'):
        # print("New Document")
        print("start parse")
        start = time.time()
        start1 = time.time()
        for idx, document in enumerate(r.read_file(file_name=path)):
            if counter2 >= 1 and counter3 >= 310000:
                # parse the document
                parsed_document = p.parse_doc(document)
                number_of_documents += 1
                # index the document data
                indexer.add_new_doc(parsed_document)
               # print("Time to parse: " + str(time.time() - start1))
               # print(counter)
                if counter == 9999:
                    print("Time to parse: " + str((time.time()- start)/10000))
                    start = time.time()
                    counter = -1
                counter += 1
                #print(counter)
            counter3 += 1
        counter2 += 1
        counter3 = 0
        print("Time to parse: " + str(time.time() - start1))

    """
   # documents_list = r.read_file(file_name='./Data/covid19_07-08.snappy.parquet')
    documents_list = r.read_file(file_name='sample3.parquet')
    for idx, document in enumerate(documents_list):
        # parse the document
        # print("after")
        parsed_document = p.parse_doc(document)  # {"donald":-1, "first":1}
        number_of_documents += 1
        # print(idx)
        # index the document data
        indexer.add_new_doc(parsed_document)

    # Iterate over every document in the file
    """

    print('Finished parsing and indexing. Starting to export files')

    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    utils.save_obj(indexer.postingDict, "posting")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    run_engine(corpus_path, output_path, stemming)
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, num_docs_to_retrieve):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
