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
        # span = self.__tokenizer.spans(buffer)
        newbuffer = self.__normalizer.canonicalize(buffer)
        for token, span in self.__tokenizer.tokens(newbuffer):
            tokenlst.append((self.__normalizer.normalize(token), span))
        root = self.__trie
        matches = []
        active = []
        for term, span in tokenlst:
            consumed = root.consume(term)
            active2 = []
                
            for active_trie, active_span, path in active:
                consumed2 = active_trie.consume(term)
                if consumed2 is None:
                    consumed2 = active_trie.consume(' '+term)
                    path += ' '
                if consumed2 is not None:
                    if consumed2.is_final():
                        matches.extend([(path+term, (active_span[0], span[1]))])
                    active2.append((consumed2, (active_span[0], span[1]), path+term))
                # active2.append((consumed, (active_span[0], span[1]), path+term))#hereee
            if consumed is not None:
                if consumed.is_final():        
                    matches.extend([(term, span)])
                active2.append((consumed, span, term))
            active = active2
                
                
        for match_str, span in matches:
            yield {"match": match_str, "surface": re.sub(r' +', ' ', buffer[span[0]:span[1]]), "span": span, "meta": root.consume(match_str).get_meta()}
        

        
        