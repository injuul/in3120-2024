# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods
# pylint: disable=too-many-locals

from collections import Counter
from typing import Iterator, Dict, Any
from .sieve import Sieve
from .ranker import Ranker
from .corpus import Corpus
from .invertedindex import InvertedIndex


class SimpleSearchEngine:
    """
    Realizes a simple query evaluator that efficiently performs N-of-M matching over an inverted index.
    I.e., if the query contains M unique query terms, each document in the result set should contain at
    least N of these m terms. For example, 2-of-3 matching over the query 'orange apple banana' would be
    logically equivalent to the following predicate:

       (orange AND apple) OR (orange AND banana) OR (apple AND banana)
       
    Note that N-of-M matching can be viewed as a type of "soft AND" evaluation, where the degree of match
    can be smoothly controlled to mimic either an OR evaluation (1-of-M), or an AND evaluation (M-of-M),
    or something in between.

    The evaluator uses the client-supplied ratio T = N/M as a parameter as specified by the client on a
    per query basis. For example, for the query 'john paul george ringo' we have M = 4 and a specified
    threshold of T = 0.7 would imply that at least 3 of the 4 query terms have to be present in a matching
    document.
    """

    def __init__(self, corpus: Corpus, inverted_index: InvertedIndex):
        self.__corpus = corpus
        self.__inverted_index = inverted_index

    def evaluate(self, query: str, options: Dict[str, Any], ranker: Ranker) -> Iterator[Dict[str, Any]]:
        """
        Evaluates the given query, doing N-out-of-M ranked retrieval. I.e., for a supplied query having M
        unique terms, a document is considered to be a match if it contains at least N <= M of those terms.

        The matching documents, if any, are ranked by the supplied ranker, and only the "best" matches are yielded
        back to the client as dictionaries having the keys "score" (float) and "document" (Document).

        The client can supply a dictionary of options that controls the query evaluation process: The value of
        N is inferred from the query via the "match_threshold" (float) option, and the maximum number of documents
        to return to the client is controlled via the "hit_count" (int) option.
        """
        match_threshold = options.get("match_threshold")
        hit_count = options.get("hit_count")

        # Split the query into terms, and compute the number of terms that must match
        query_terms = [term for term in self.__inverted_index.get_terms(query)]
        match_count = int(len(query_terms) * match_threshold)

        # Compute the document scores for the query
        sieve = Sieve(hit_count)
        iterators = []
        for term in query_terms:
            print(term)
            iterators.append(self.__inverted_index.get_postings_iterator(term)) # iterator : posting
        
        
        postings = [next(iterator, None) for iterator in iterators]
        while None in postings:
            postings.remove(None)
        
        while len(postings) >= match_count:
            most_common = Counter(posting.document_id for posting in postings).most_common(1)[0]
            print('\n\nheree,',most_common)
            if most_common == match_count:
                ranker.reset(most_common.document_id)
                for query in query_terms:
                    ranker.update(term, len(query_terms), most_common)
                sieve.sift(ranker.evaluate(), most_common)
                for _ in range(match_count):
                    index = postings.index(most_common)
                    postings[index] = next(iterators[index], None)
                    if postings[index] is None:
                        postings.remove(None)
                continue
            # if len(set(posting.document_id for posting in postings)) == len(query_terms)-match_count:
            #     counter = Counter(posting.document_id for posting in postings)
            #     common = counter.most_common(1)[0]  
            #     ranker.reset(common.document_id)
            #     for query in query_terms:
            #         ranker.update(term, len(query_terms), common)
            #     sieve.sift(ranker.evaluate(), common)
            #     for _ in range(match_count):
            #         index = postings.index(common)
            #         postings[index] = next(iterators[index], None)
            #         if postings[index] is None:
            #             postings.remove(None)
                continue
            min_posting = min(postings, key=lambda x: x.document_id)
            min_posting_index = postings.index(min_posting)
            postings[min_posting_index] = next(iterators[min_posting_index], None)
            if None in postings:
                postings.remove(None)            
        
        for score, doc in sieve.winners():
            yield {"score": score, "document": self.__corpus.get_document(doc)}