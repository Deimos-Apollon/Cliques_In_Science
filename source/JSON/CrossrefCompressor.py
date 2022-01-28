class CrossrefCompressor:
    def __is_valid(self, work):
        if work.get('author') and work.get('DOI') and work.get('reference') and work.get['subject'] \
                and work.get('published-print') and work.get("type"):
            if work['published-print'].get('date-parts'):
                for author in work['author']:
                    if self.__is_valid_author(author):
                        return True
        return False

    @staticmethod
    def __is_valid_author(author):
        if author.get('given') and author.get('family'):
            given, family = author['given'], author['family']
            if not any(x in given for x in ['"', ","]) and not any(x in family for x in ['"', ","]) and \
                    len(given) < 40 and len(family) < 40:
                return True
        return False
