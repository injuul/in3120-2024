# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods

import re
from typing import Iterator, Dict, Any, List, Tuple
from .normalizer import Normalizer
from .tokenizer import Tokenizer
from .trie import Trie


class StringFinder:
    """
    Given a trie encoding a dictionary of strings, efficiently finds the subset of strings in the dictionary
    that are also present in a given text buffer. I.e., in a sense computes the "intersection" or "overlap"
    between the dictionary and the text buffer.

    Uses a trie-walk algorithm similar to the Aho-Corasick algorithm with some simplifications and some minor
    NLP extensions. The running time of this algorithm is virtually independent of the size of the dictionary,
    and linear in the length of the buffer we are searching in.

    The tokenizer we use when scanning the input buffer is assumed to be the same as the one that was used
    when adding strings to the trie.
    """

    def __init__(self, trie: Trie, normalizer: Normalizer, tokenizer: Tokenizer):
        self.__trie = trie
        self.__normalizer = normalizer  # The same as was used for trie building.
        self.__tokenizer = tokenizer  # The same as was used for trie building.

    def scan(self, buffer: str) -> Iterator[Dict[str, Any]]:
        """
        Scans the given buffer and finds all dictionary entries in the trie that are also present in the
        buffer. We only consider matches that begin and end on token boundaries.

        The matches, if any, are yielded back to the client as dictionaries having the keys "match" (str),
        "surface" (str), "meta" (Optional[Any]), and "span" (Tuple[int, int]). Note that "match" refers to
        the matching dictionary entry, "surface" refers to the content of the input buffer that triggered the
        match (the surface form), and "span" refers to the exact location in the input buffer where the surface
        form is found. Depending on the normalizer that is used, "match" and "surface" may or may not differ.

        A space-normalized version of the surface form is emitted as "surface", for convenience. Clients
        that require an exact surface form that is not space-normalized can easily reconstruct the desired
        string using the emitted "span" value.

        In a serious application we'd add more lookup/evaluation features, e.g., support for prefix matching,
        support for leftmost-longest matching (instead of reporting all matches), and more.
        """
        tokenlst = []
        span = self.__tokenizer.spans(buffer)
        newbuffer = self.__normalizer.canonicalize(buffer)
        for token, _ in self.__tokenizer.tokens(newbuffer):
            tokenlst.append((self.__normalizer.normalize(token), _))
        tokens = self.__tokenizer.join(tokenlst)
    
        root = self.__trie
        lst = []

        for term, _ in self.__tokenizer.tokens(tokens):
            span0 =  next(span)
            if root.__contains__(term):
                endnode = root.__getitem__(term)  
                for tail in endnode.__iter__():      
                    if tail is not None:
                        #calculates the length to get the whole term
                        length = newbuffer[span0[0]:span0[1]+len(tail)].count(' ')-re.sub(' +', ' ', newbuffer[span0[0]:span0[1]+len(tail)]).count(' ')
                        lst.extend([(term+tail,re.sub(' +', ' ', newbuffer[span0[0]:span0[1]+len(tail)+length]), (span0[0], span0[1]+len(tail)+length))])
                    else:
                        lst.extend([(term, buffer[span0[0]:span0[1]], span0)])

        for match_str, surface, span in lst:
            if type(match_str) == str:
                print({'match': match_str, 'surface': surface, 'span': span})
                yield {"match": match_str, "surface": surface, "span": span, "meta": root.__getitem__(match_str).get_meta()}
        

        
        