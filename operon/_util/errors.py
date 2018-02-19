class MalformedPipelineError(SystemExit):
    """
    Exception for when the developer doesn't properly override a
    necessary method of the ParslPipeline class
    """
    def __init__(self, msg: str, *args: object) -> None:
        msg = 'Malformed pipeline: ' + msg
        super().__init__(msg, *args)
