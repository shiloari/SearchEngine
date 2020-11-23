import os
import time
import unicodedata
import matplotlib.pyplot as plt
from numpy import unicode

from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import glob
from pathlib import Path


def saveAsJSON(path, file_name, to_be_saved):
    import json

    with open(path+'/'+file_name+'.json', 'w') as file:
        json.dump(to_be_saved, file)
        file.close()

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
    start = time.time()

    # folders = 0
    # for _, dirnames, _ in os.walk(corpus_path):
    #     # ^ this idiom means "we won't be using this value"
    #     folders += len(dirnames)
    # progressBar = ''
    # for i in range(folders):
    #     progressBar += ' '
    # progressBar = '[' + progressBar + ']'
    # print(progressBar, ' 0%')
    for path in Path(corpus_path).rglob('*.parquet'):
        # print("New Document")
        print("start parse parquet")
        start1 = time.time()
        sizeOfCorpus = 0
        counter = 0
        for idx, document in enumerate(r.read_file(file_name=path)):
                if sizeOfCorpus == 1:
                    break
                if counter > 9999:
                    counter = 0
                    print("parsed 10000 files")
                # print(idx)
                # parse the document
                parsed_document = p.parse_doc(document)
                number_of_documents += 1
                # index the document data
                indexer.add_new_doc(parsed_document)
                counter += 1
        print("Time to parse parquet: " + str(time.time() - start1))
        sizeOfCorpus += 1
        # progressBar = progressBar[:idx] + '\x1b[6;30;42m' + 'X' + '\x1b[0m]' + progressBar[idx:]
        # print(progressBar, ' ',  float(counter/folders),' %', end='\r')
    print("Total time to parse: " , time.time()-start)

    #### save as json

    print('Finished parsing and indexing. Starting to export files')

    saveAsJSON(Path.absolute(), 'inverted_idx', indexer.inverted_idx)
    saveAsJSON(Path.absolute(), 'posting' , indexer.postingDict)
    # utils.save_obj(indexer.inverted_idx, "inverted_idx")
    # utils.save_obj(indexer.postingDict, "posting")


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
