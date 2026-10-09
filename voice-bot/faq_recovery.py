"""One-shot recovery for an unfinished, fact-grounded FAQ after transcriptless VAD.
Never retries an LLM, booking, channel action or arbitrary assistant text.
"""
class FAQReplyRecovery:
    def __init__(self):
        self.reply = None
        self.interrupted = False
        self.clarified = False

    def new_transcript(self):
        self.clarified = False
        self.reply = None
        self.interrupted = False

    def commit(self, reply):
        self.reply = reply
        self.interrupted = False

    def interrupt(self):
        if self.reply:
            self.interrupted = True

    def audio_completed(self):
        self.reply = None
        self.interrupted = False

    def take_after_empty_turn(self):
        if not self.interrupted:
            return None
        reply = self.reply
        self.reply = None
        self.interrupted = False
        return reply

    def take_clarification(self):
        if self.clarified:
            return False
        self.clarified = True
        return True
