import pyodbc
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from datetime import datetime
import sys

class ChatHistory:
    """Simple chat history tracking user and assistant messages."""
    def __init__(self):
        self.messages = []

    def add_user_message(self, content):
        self.messages.append({"role": "user", "content": content})

    def add_assistant_message(self, content):
        self.messages.append({"role": "assistant", "content": content})

    def clear_history(self):
        self.messages = []

    def __iter__(self):
        return iter(self.messages)

    def __len__(self):
        return len(self.messages)

    def remove_range(self, start, end):
        del self.messages[start:end]


def fetch_tables(conn_str):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    query = """
        SELECT 
            s.name AS schema_name,
            t.name AS table_name
        FROM 
            sys.tables t
        INNER JOIN 
            sys.schemas s ON t.schema_id = s.schema_id
        ORDER BY 
            s.name, t.name;
    """
    cursor.execute(query)

    tables = set()
    for schema_name, table_name in cursor.fetchall():
        tables.add(f"{schema_name}.{table_name}")

    cursor.close()
    conn.close()

    return "\n".join(sorted(tables))


def build_system_message(tables_description):
    return f"""
I am an agent running Mistral focusing on the tables below, how can I help?

The tables in the database are:

{tables_description}

Use a professional tone when answering and provide a summary of data instead of lists.
If users ask about topics you don't know, answer that you don't know. Today's date is {datetime.now():%Y-%m-%d}.
You must answer providing a list of tables that must be used to answer the question and an explanation of what you'll be doing to answer the question.
You must use the provided tool to query the database.
If the request is complex, break it down into smaller steps and call the plugin as many times as needed. Ideally, don't use more than 5 tables in the same query.
"""


def main():
    # Your connection string here
    conn_str = (
        r"Driver={ODBC Driver 18 for SQL Server};"
        r"Server=NZXT\SQL2025;"
        r"Database=StackOverflow2013;"
        r"Trusted_Connection=yes;"
        r"TrustServerCertificate=yes;"
    )

    print("Connecting to SQL Server...")
    tables_description = fetch_tables(conn_str)
    print("Connected.\n")

    system_message = build_system_message(tables_description)

    llm = ChatOllama(model="mistral", temperature=0)

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_message),
        ("human", "{input}")
    ])

    chain = prompt | llm

    chat = ChatHistory()

    print("Natural Language Database Chatbot Agent v1.0")
    print("Type your question or commands:")
    print("Commands: /c (clear screen), /ch (clear chat history), /h (show chat history), /exit (quit)\n")

    while True:
        try:
            user_input = input(": ").strip()

            if not user_input:
                continue

            if user_input.lower() in ("/exit", "exit", "quit"):
                print("Goodbye!")
                break

            if user_input == "/c":
                # Clear screen
                print("\033[2J\033[H", end="")  # ANSI escape codes for clear screen and cursor home
                continue

            if user_input == "/ch":
                chat.clear_history()
                print("Chat history cleared.")
                continue

            if user_input == "/h":
                if len(chat) == 0:
                    print("No chat history.")
                else:
                    for idx, message in enumerate(chat):
                        role = message["role"]
                        content = message["content"]
                        print(f"[{idx+1}] {role.upper()}:\n{content}\n{'-'*40}")
                continue

            # Normal user question processing
            chat.add_user_message(user_input)
            print(": Thinking...")

            response = chain.invoke({"input": user_input})

            print(response)
            chat.add_assistant_message(response)

        except KeyboardInterrupt:
            print("\nInterrupted by user. Exiting.")
            sys.exit(0)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
