import math
import os
import time
import indexer
import local_method
import ranker
import reader
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from pathlib import Path
import json


def updateVectorsFile(doc_id, data, inv_index, num_of_docs_in_corpus, vectorsDict):   #termsData = {term: [[doc_id, tweet_id, d, |d|]]}
    """
    :param doc_id: doc id
    :param data: the vector to append
    :param inv_index: inv_index
    :param num_of_docs_in_corpus: #words
    :param vectorsDict: the vectors dictionary to append to.
    :return:
    """
    values = list(data[3].values())
    vectorsDict[doc_id] = [{}, data[0]]
    for term in data[3].keys():
        #for each term, compute tf_idf and append to vector dictionary
        corrected_term = ranker.get_correct_term(term, inv_index)
        tf = data[3][term]/len(values)
        idf = math.log(num_of_docs_in_corpus / len(inv_index[corrected_term][1]), 2)
        tf_idf = tf*idf
        vectorsDict[doc_id][0][corrected_term] = tf_idf

def clearSingleEntities(inv_index, parser, output_path, num_of_docs_in_corpus):
    """
    :param inv_index: inv_index
    :param parser: parser
    :param output_path: output_path
    :param num_of_docs_in_corpus: #docs in corpus
    :return:
    """
    EntitiesDict = {}       #{doc_id: [term1,term2]}
    docs_to_clear = {}      # {pkl_id: [doc1 ,doc2]}
    vectorsDict = {}    # {doc_id: normalized vectors}
    # for each term in inv_index, check if should be cleared up.
    # term will be cleared if it is a single entity or term in whole corpus.
    for term in inv_index.keys():
        if inv_index[term][0] == 1:
            single_doc = inv_index[term][1][0]
            if single_doc in EntitiesDict.keys():
                EntitiesDict[single_doc].append(term)
            else:
                EntitiesDict[single_doc] = [term]
    # if there's no entities to remove, return.
    if len(EntitiesDict.keys()) == 0:
        return
    sorted_keys = sorted(EntitiesDict.keys())   # all docs to clear
    key_num = int(sorted_keys[0] / indexer.postingSize)
    docs_to_clear[key_num] = []
    for doc_id in sorted_keys:
        if doc_id >= (key_num + 1) * indexer.postingSize:  # should get new data, update key_num
            key_num = int(doc_id / indexer.postingSize)
            docs_to_clear[key_num] = [doc_id]
        else:
            docs_to_clear[key_num] += [doc_id]

    for pkl_key in docs_to_clear.keys():
        data = utils.load_obj(output_path + '/PostingFiles/' + str(pkl_key))
        for doc_id in data.keys(): #key is now a string
            doc_idint = int(doc_id)
            if doc_idint in EntitiesDict.keys():
                for entity in EntitiesDict[doc_idint]:
                    if len(data[doc_id][3]) >= 5 or parser.isEntity(entity):
                        data[doc_id][1] -= data[doc_id][3][entity]
                        data[doc_id][3].pop(entity)
                        inv_index.pop(entity)
            values = data[doc_id][3].values()
            if len(values) != 0:
                data[doc_id][2] = max(values)
            else:
                data[doc_id][2] = 0
            updateVectorsFile(doc_id, data[doc_id], inv_index, num_of_docs_in_corpus, vectorsDict)
        utils.save_obj(data, output_path + '/PostingFiles/' + str(pkl_key))
    utils.save_obj(vectorsDict, output_path + '/PostingFiles/vectorsFile')
    vectorsDict.clear()
    docs_to_clear.clear()
    EntitiesDict.clear()


def run_engine(config):
    """
    :return:
    """
    number_of_documents = 0
    output_path = config.savedFileMainFolder
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse(config.toStem)
    m_Indexer = Indexer(output_path)
    parquetPaths = []
    for (dirPath, dirNames, fileNames) in os.walk(config.get__corpusPath()):
        for fileName in fileNames:
            parquetPaths.append((dirPath + '\\' + fileName))
    for i in range(len(parquetPaths)):
        parquetPaths[i] = parquetPaths[i][parquetPaths[i].find('\\') + 1:]
        if ".DS_Store" in parquetPaths[i]:
            continue
        parquet = r.read_file(file_name=parquetPaths[i])
        for document in parquet:
            number_of_documents += 1
            parsed_document = p.parse_doc(document)
            # index the document data
            m_Indexer.add_new_doc(parsed_document)
    # if there's more postings to flush, do it.
    if len(m_Indexer.postingDictionary) > 0:
        utils.save_obj(m_Indexer.postingDictionary, m_Indexer.postingsPath + '/' + str(m_Indexer.pkl_key))
    # Clear single terms and entities, updated inverted index to disk.
    clearSingleEntities(m_Indexer.inverted_idx, p, output_path, m_Indexer.num_of_docs_in_corpus)
    utils.save_obj(m_Indexer.inverted_idx, output_path + '/inverted_idx')
    m_Indexer.inverted_idx.clear()
    utils.save_obj(number_of_documents,output_path+'/PostingFiles/num_of_docs_in_corpus')

def load_index(output_path):
    inverted_idx = utils.load_obj(output_path + '/inverted_idx')
    return inverted_idx

def search_and_rank_query(query, inverted_index, k, output_path, vectorDict,stemming):
    p = Parse(stemming)
    # parse query.
    query_as_dict = p.parse_sentence(query, term_dict={})
    if len(query_as_dict.keys())==0:
        return []
    searcher = Searcher(inverted_index,output_path)
    # search for relevant docs given the query. min threshold is 100 docs.
    relevant_docs = searcher.relevant_docs_from_posting(query_as_dict, 100, output_path)
    # rank those docs and get the top 100 of them.
    ranked_docs, sorted_keys = searcher.ranker.rank_relevant_doc(relevant_docs, query_as_dict, inverted_index, output_path, vectorDict)  # { doc: 4, doc: 10}
    top_100_keys = searcher.ranker.retrieve_top_k(sorted_keys, 100)
    # build association matrix and expand the query.
    expanded_query = local_method.build_association_matrix(inverted_index, query_as_dict, top_100_keys, vectorDict)
    # search again, with the expanded query.
    relevant_docs = searcher.relevant_docs_from_posting(expanded_query, k, output_path)
    # rank again and return the top K (given input) ranked.
    ranked_docs, sorted_keys = searcher.ranker.rank_relevant_doc(relevant_docs, expanded_query, inverted_index, output_path, vectorDict)  # { doc: 4, doc: 10}
    top_k_keys = searcher.ranker.retrieve_top_k(sorted_keys, k)
    top_K = []
    for doc_id in top_k_keys:
        top_K.append(ranked_docs[doc_id])
    return top_K

def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    confing = ConfigClass(corpus_path, output_path, stemming)
    # run engine: parse, index, and write to disk
    run_engine(confing)
    # read querires
    if not isinstance(queries, list):
        r = reader.ReadFile(corpus_path)
        queries = r.read_queries(queries) # queries is a path to txt file.
    # load inverted index and vectors file
    vectorsFile = utils.load_obj(output_path + '/PostingFiles/vectorsFile')
    inverted_index = load_index(output_path)
    # get all top ranked docs related to query.
    for query in queries:
        for doc_tuple in search_and_rank_query(query, inverted_index, num_docs_to_retrieve, output_path+"/PostingFiles", vectorsFile,stemming):
            print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
    # all_results = []
    # for i, query in enumerate(queries):
    #     for doc_tuple in search_and_rank_query(queries, inverted_index, num_docs_to_retrieve, output_path+"/PostingFiles", vectorsFile):
    #         all_results.append((i+1, str(doc_tuple[0]), str(doc_tuple[1])))
    #utils.write_csv(all_results,output_path)