# pylint: disable=missing-module-docstring

from typing import Iterator
from .posting import Posting
import dis

class PostingsMerger:
    """
    Utility class for merging posting lists.

    It is currently left unspecified what to do with the term frequency field
    in the returned postings when document identifiers overlap. Different
    approaches are possible, e.g., an arbitrary one of the two postings could
    be returned, or the posting having the smallest/largest term frequency, or
    a new one that produces an averaged value, or something else.

    Note that the result of merging posting lists is itself a posting list.
    Hence the merging methods can be combined to compute the result of more
    complex Boolean operations over posting lists.
    """

    @staticmethod
    def intersection(iter1: Iterator[Posting], iter2: Iterator[Posting]) -> Iterator[Posting]:
        """
        A generator that yields a simple AND(A, B) of two posting
        lists A and B, given iterators over these.

        In set notation, this corresponds to computing the intersection
        D(A) ∩ D(B), where D(A) and D(B) are the sets of documents that
        appear in A and B: A posting appears once in the result if and
        only if the document referenced by the posting appears in both
        D(A) and D(B).

        The posting lists are assumed sorted in increasing order according
        to the document identifiers.
        """
        try:
            b = next(iter2)
        except:
            return
        try:
            a = next(iter1)
        except:
            return
        while True:
            if a.document_id == b.document_id:
                a.term_frequency += b.term_frequency
                yield a
                try:
                    a,b = next(iter1), next(iter2)
                except:
                    break
            elif a.document_id < b.document_id:
                try:
                    a = next(iter1)
                except:
                    break
            else:
                try:
                    b = next(iter2)
                except:
                    break
        
        
    @staticmethod
    def union(iter1: Iterator[Posting], iter2: Iterator[Posting]) -> Iterator[Posting]:
        """
        A generator that yields a simple OR(A, B) of two posting
        lists A and B, given iterators over these.

        In set notation, this corresponds to computing the union
        D(A) ∪ D(B), where D(A) and D(B) are the sets of documents that
        appear in A and B: A posting appears once in the result if and
        only if the document referenced by the posting appears in either
        D(A) or D(B).

        The posting lists are assumed sorted in increasing order according
        to the document identifiers.
        """
        try:
            try:
                b = next(iter2)
            except:
                yield from iter1
                return
            try:
                a = next(iter1)
            except:
                yield b
                yield from iter2
                return
        except:
            return
        
        while True:
            if a.document_id < b.document_id:
                yield a
                try:
                    a = next(iter1)
                except:
                    yield b
                    yield from iter2
                    return
            elif a.document_id > b.document_id:
                yield b
                try:
                    b = next(iter2)
                except:
                    yield a
                    yield from iter1
                    return
            else:
                a.term_frequency += b.term_frequency
                yield a
                try:
                    try:
                        b = next(iter2)
                    except:
                        yield from iter1
                        return
                    try:
                        a = next(iter1)
                    except:
                        yield b
                        yield from iter2
                        return
                except:
                    return

    @staticmethod
    def difference(iter1: Iterator[Posting], iter2: Iterator[Posting]) -> Iterator[Posting]:
        """
        A generator that yields a simple ANDNOT(A, B) of two posting
        lists A and B, given iterators over these.

        In set notation, this corresponds to computing the difference
        D(A) - D(B), where D(A) and D(B) are the sets of documents that
        appear in A and B: A posting appears once in the result if and
        only if the document referenced by the posting appears in D(A)
        but not in D(B).

        The posting lists are assumed sorted in increasing order according
        to the document identifiers.
        """
        
        try:
            b = next(iter2)
        except:
            yield from iter1
            return
        for a in iter1:
            while a.document_id > b.document_id:
                try:
                    b = next(iter2)
                except:
                    if a.document_id > b.document_id:
                        yield a
                    yield from iter1
                    return
            if a.document_id == b.document_id:
                b = next(iter2)
                continue
            else:
                yield a
            
  