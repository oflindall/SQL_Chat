class ChatHistory:
    def __init__(self):
        self.messages = []

    def add_user_message(self, content):
        self.messages.append({"role": "user", "content": content})

    def add_assistant_message(self, content):
        self.messages.append({"role": "assistant", "content": content})

    def clear_history(self):
        self.messages = []

    def get_history(self):
        return self.messages

    def __iter__(self):
        return iter(self.messages)

    def __len__(self):
        return len(self.messages)
