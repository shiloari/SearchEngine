import json
import os
import time
import traceback

import ranker
from indexer import Indexer
from parser_module import Parse
from ranker import Ranker
import utils
import numpy as np

class Searcher:

    def __init__(self, inverted_index, output_path):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index
        self.num_of_docs_in_corpus = utils.load_obj(output_path + '/num_of_docs_in_corpus')
        i = 4
        while self.num_of_docs_in_corpus % i != 0:
            i += 1
        self.partition_num_of_docs = i
        self.partial_size = int(self.num_of_docs_in_corpus/self.partition_num_of_docs)
        self.output_path = output_path

    def setDocsMatrix(self, query_as_dict, j, k_intersection, writeToMemo, readFromMemo):
        """
        :param query_as_dict: query as dictionary
        :param j: the j column in matrix
        :param k_intersection: the size of docs intersection
        :param writeToMemo: boolean
        :param readFromMemo: boolean
        :return:
        """

        matrix = np.zeros((len(query_as_dict.keys()), self.partial_size))
        if readFromMemo:
            matrix = utils.load_obj(self.output_path+'/matrix_'+str(j))
            column_sums = matrix.sum(axis=0)
            return list(np.argwhere(column_sums >= k_intersection).flatten())

        for i, term in enumerate(query_as_dict):
            corrected_term = ranker.get_correct_term(term, self.inverted_index)
            if corrected_term is None:
                continue
            relevant_docs = list(
                filter(lambda x:  self.partial_size * j <= x < self.partial_size * (j + 1), self.inverted_index[corrected_term][1]))
            for index in range(len(relevant_docs)):
                relevant_docs[index] -= self.partial_size * j
            np.put(matrix[i], relevant_docs, 1)
        if writeToMemo:
            utils.save_obj(matrix, self.output_path+'/matrix_'+str(j))
        column_sums = matrix.sum(axis=0)
        to_be_returned = list(np.argwhere(column_sums >= k_intersection).flatten())
        for i in range(len(to_be_returned)):
            to_be_returned[i] += self.partial_size * j
        return to_be_returned

    def intersectUsingIO(self, all_intersections, query_as_dict, threshold ):
        for j in range(0, self.partition_num_of_docs):
            all_intersections += self.setDocsMatrix(query_as_dict, j, 1, True, False)
        for k_intersection in range(2, len(query_as_dict.keys())):
            k_intersection_docs = []
            for j in range(0, self.partition_num_of_docs):
                k_intersection_docs += self.setDocsMatrix(query_as_dict, j, k_intersection, False, True)
            if len(k_intersection_docs) < threshold:
                break
            else:
                all_intersections = k_intersection_docs
        return all_intersections

    def intersectWithoutIO(self, all_intersections, query_as_dict, threshold):
        for j in range(0, self.partition_num_of_docs):
            all_intersections += self.setDocsMatrix(query_as_dict, j, 1, False, False)
        for k_intersection in range(2, len(query_as_dict.keys())):
            k_intersection_docs = []
            for j in range(0, self.partition_num_of_docs):
                k_intersection_docs += self.setDocsMatrix(query_as_dict, j, k_intersection, False, False)
            if len(k_intersection_docs) < threshold:
                break
            else:
                all_intersections = k_intersection_docs
        return all_intersections


    def getMaxOccurrences(self, query_as_dict):
        max = 0
        for term in query_as_dict.keys():
            corrected_term = ranker.get_correct_term(term, self.inverted_index)
            if corrected_term is None:
                continue
            current_len = len(self.inverted_index[corrected_term])
            if current_len > max:
                max = current_len
        return max

    def relevant_docs_from_posting(self, query_as_dict, threshold, output_path):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        all_intersections = []
        if len(query_as_dict.keys()) == 1:
            corrected_term = ranker.get_correct_term(list(query_as_dict.keys())[0], self.inverted_index)
            return self.inverted_index[corrected_term][1]
        max_num_of_docs = self.getMaxOccurrences(query_as_dict)
        if max_num_of_docs > 80000:
            all_intersections = self.intersectWithoutIO(all_intersections, query_as_dict, threshold)
        else:
            all_intersections = self.intersectUsingIO(all_intersections, query_as_dict,threshold)
        return all_intersections

