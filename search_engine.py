import os
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
import json


def saveAsJSON(path, file_name, to_be_saved):
    file = open(path + "/" + file_name + ".json", "a")
    json.dump(to_be_saved, file, indent=4, sort_keys=True)
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

    # folders = 0
    # for _, dirnames, _ in os.walk(corpus_path):
    #     # ^ this idiom means "we won't be using this value"
    #     folders += len(dirnames)
    # progressBar = ''
    # for i in range(folders):
    #     progressBar += ' '
    # progressBar = '[' + progressBar + ']'
    # print(progressBar, ' 0%')
    startCorpus = time.time()
    start = time.time()
    sizeOfCorpus = 0
    for path in Path(corpus_path).rglob('*.parquet'):
        parsingTime = 0
        indexingTime = 0
        # print("New Document")
        if sizeOfCorpus == 3:
            break
        print("start parse parquet")
        start1 = time.time()
        counter = 0
        for idx, document in enumerate(r.read_file(file_name=path)):
                # if sizeOfCorpus < 7 :
                #     counter += 1
                #     continue
                # if counter <150000:
                #     counter += 1
                #     continue
                # if counter > 9999:
                #     print("parsed 10000 files in average time: ", (time.time()-start1)/10000)
                #     counter = 0
                #     start1 = time.time()
                # print(idx)
                # parse the document
                startParse = time.time()
                parsed_document = p.parse_doc(document)
                parsingTime += time.time() - startParse
                number_of_documents += 1
                # index the document data
                startIndex = time.time()
                indexer.add_new_doc(parsed_document)
                indexingTime += time.time() - startIndex
                counter += 1
        print('-------------------------------------------------------------')
        print("Time to whole parse parquet: " + str(time.time() - start))
        print("Average time to parse tweet: " + str((time.time() - start)/counter))
        print('Parsing total time: ', parsingTime, ' | Indexing total time: ', indexingTime)
        print('-------------------------------------------------------------')
        start = time.time()
        sizeOfCorpus += 1
        # progressBar = progressBar[:idx] + '\x1b[6;30;42m' + 'X' + '\x1b[0m]' + progressBar[idx:]
        # print(progressBar, ' ',  float(counter/folders),' %', end='\r')
    print("End and start to full flush !")
    start22 = time.time()
    indexer.flushAll()
    print("Total time to Flush: ",time.time() - start22)
    print("Total time to parse and index: ", time.time()-startCorpus)
    #### save as json

    print('Finished parsing and indexing. Starting to export files')

    saveAsJSON('.', 'inverted_idx', indexer.inverted_idx)
    # saveAsJSON('.', 'posting' , indexer.postingDictionary)
    # utils.save_obj(indexer.inverted_idx, "inverted_idx")
    # utils.save_obj(indexer.postingDict, "posting")


def load_index():
    print('Load inverted index')
    # inverted_index = ("inverted_idx")
    with open("./inverted_idx.json") as file:
        inverted_idx = json.load(file)
    return inverted_idx


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query, term_dict={})
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)  # { doc: 4, doc: 10}
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    #run_engine(corpus_path, output_path, stemming)
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, num_docs_to_retrieve):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
