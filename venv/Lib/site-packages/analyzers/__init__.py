"""Suggest Analyzer and Auto Complete Filter Class."""
from elasticsearch_dsl import analyzer, token_filter

other_param = dict()


class Suggest(object):
    """
    Suggest class.

    usage example:
        >> suggest_obj = Suggest()
        >> suggestion = suggest_obj.suggest()
        >> autocomplete = suggest_obj.autocomplete()
    """

    other_param.update({"min_gram": 1, "max_gram": 20})

    def suggest():
        """Suggest method which used edge n-gram analyzer."""
        suggestfilter = token_filter("suggestfilter", type="ngram",
                                     **other_param)

        bestsuggest = analyzer("bestsuggest", tokenizer="standard",
                               filter=["lowercase", suggestfilter,
                                       "asciifolding"])
        return bestsuggest

    def autocomplete():
        """Autocomplete method uses n-gram analyzer."""
        autocompletefilter = token_filter("autocompletefilter",
                                          type="edge_ngram", **other_param)
        autocomplete = analyzer("autocomplete", tokenizer="standard",
                                filter=["lowercase", autocompletefilter])
        return autocomplete
