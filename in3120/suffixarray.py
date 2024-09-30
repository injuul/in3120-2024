# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long

import sys
from bisect import bisect_left
from itertools import takewhile
from typing import Any, Dict, Iterator, Iterable, Tuple, List
from collections import Counter
from .corpus import Corpus
from .normalizer import Normalizer
from .tokenizer import Tokenizer


class SuffixArray:
    """
    A simple suffix array implementation. Allows us to conduct efficient substring searches.
    The prefix of a suffix is an infix!

    In a serious application we'd make use of least common prefixes (LCPs), pay more attention
    to memory usage, and add more lookup/evaluation features.
    """

    def __init__(self, corpus: Corpus, fields: Iterable[str], normalizer: Normalizer, tokenizer: Tokenizer):
        self.__corpus = corpus
        self.__normalizer = normalizer
        self.__tokenizer = tokenizer
        self.__haystack: List[Tuple[int, str]] = []  # The (<document identifier>, <searchable content>) pairs.
        self.__suffixes: List[Tuple[int, int]] = []  # The sorted (<haystack index>, <start offset>) pairs.
        self.__build_suffix_array(fields)  # Construct the haystack and the suffix array itself.

    def __build_suffix_array(self, fields: Iterable[str]) -> None:
        """
        Builds a simple suffix array from the set of named fields in the document collection.
        The suffix array allows us to search across all named fields in one go.
        """
        for document in self.__corpus:
            for field in fields:
                offset = 0
                term =  self.__normalize(document[field])
                if term:
                    self.__haystack.append((document.get_document_id(), term))
                    for part in range(len(term.split(' '))):
                        offset = len(part)+1
                        self.__suffixes.append((len(self.__haystack) - 1, offset))
        self.__haystack.sort(key=lambda x: x[1])
        self.__suffixes = [(self.__haystack[i], self.__suffixes[i][1]) for i in range(len(self.__suffixes))]
        # self.__suffixes.sort(key=lambda x: self.__haystack[x[0]][1])
                
             

    def __normalize(self, buffer: str) -> str:
        """
        Produces a normalized version of the given string. Both queries and documents need to be
        identically processed for lookups to succeed.
        """
        tokens = self.__tokenizer.strings(self.__normalizer.canonicalize(buffer))
        token = (self.__normalizer.normalize(t) for t in tokens)
        return " ".join(token)

    def __binary_search(self, needle: str) -> int:
        """
        Does a binary search for a given normalized query (the needle) in the suffix array (the haystack).
        Returns the position in the suffix array where the normalized query is either found, or, if not found,
        should have been inserted.

        Kind of silly to roll our own binary search instead of using the bisect module, but seems needed
        prior to Python 3.10 due to how we represent the suffixes via (index, offset) tuples. Version 3.10
        added support for specifying a key.
        """
        # return bisect_left(self.__suffixes, needle)
        
        haystack_index = bisect_left(self.__haystack, needle, key=lambda x: x[1])
        return haystack_index

        # if index == len(self.__suffixes) or self.__suffixes[index][1] != needle[1]:
        #     self.__suffixes.insert(index, needle)
        
        
        # low = 0
        # high = len(self.__suffixes)
        # while low < high:
        #     mid = (low + high) // 2
        #     if self.__haystack[self.__suffixes[mid][0] ][1].startswith(needle):
        #         return mid
        #     elif self.__haystack[self.__suffixes[mid][0]][1] < needle:
        #         low = mid + 1
        #     else:
        #         high = mid
        # return 
        
    def evaluate(self, query: str, options: dict) -> Iterator[Dict[str, Any]]:
        """
        Evaluates the given query, doing a "phrase prefix search".  E.g., for a supplied query phrase like
        "to the be", we return documents that contain phrases like "to the bearnaise", "to the best",
        "to the behemoth", and so on. I.e., we require that the query phrase starts on a token boundary in the
        document, but it doesn't necessarily have to end on one.

        The matching documents are ranked according to how many times the query substring occurs in the document,
        and only the "best" matches are yielded back to the client. Ties are resolved arbitrarily.

        The client can supply a dictionary of options that controls this query evaluation process: The maximum
        number of documents to return to the client is controlled via the "hit_count" (int) option.

        The results yielded back to the client are dictionaries having the keys "score" (int) and
        "document" (Document).
        """
        query = self.__normalize(query)
        if not query:
            return
        # print('querry',query)
        haystrt = self.__binary_search(query)
        # print('haystrt',haystrt)
        hayend = self.__binary_search(query+chr(sys.maxunicode))
        hayrange = range(haystrt, hayend+1)
        # print('hayrange',hayrange)
        # for i in hayrange:
        #     print(i)
        docids = [self.__haystack[i][0] for i in hayrange]
        print('docids',docids)
        # print('documents',self.__corpus.get_document(docids[0]))
        max_count = options.get("hit_count", 5)
        for docid, count in Counter(docids).most_common(max_count):
            # print({"score": count, "document": self.__corpus.get_document(docid)})
            yield {"score": count, "document": self.__corpus.get_document(docid)}
            

