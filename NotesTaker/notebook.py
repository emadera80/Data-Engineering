import datetime 

last_id = 0 

class Note: 
    """
        Represent a note in the notebook Match against a string in searches and stroe tags for each note 
    """

    def __init__(self, memo, tags=""): 
        """
        initialized a note with memo and optional space-separated tags.
        Automatically set the notes creation date and a unique id 
        """

        self.memo = memo
        self.tags = tags 
        self.creation_date = datetime.date.today()
        global last_id 
        last_id +=1 
        self.id = last_id 

    def match(self, filter): 
        """
        determine if this note matches the filter text. return true if it matches false otherwise. 

        search is case sentive and matches both text and tags 
                """

        return filter in self.memo or filter in self.tags 

        