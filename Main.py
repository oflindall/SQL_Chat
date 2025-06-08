import sys
import re
import pyodbc
from Database import Database
from Chat import ChatAgent
from Chat_History import ChatHistory

def execute_sql_query(conn_str, sql_query):
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    cursor.execute(sql_query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return columns, rows

def clean_sql_query(raw_sql):
    return re.sub(r"```(?:sql)?|```", "", raw_sql).strip()

def main():
    conn_str = (
        r"Driver={ODBC Driver 18 for SQL Server};"
        r"Server=NZXT\SQL2025;"
        r"Database=AdventureWorks2022;"
        r"Trusted_Connection=yes;"
        r"TrustServerCertificate=yes;"
    )

    db = Database(conn_str)
    chat_agent = ChatAgent()
    chat_history = ChatHistory()

    print("Multi-Agent Natural Language to SQL Agent v1.0")
    print("Commands: /exit, /ch (clear chat history), /c (clear screen), /h (show chat history)\n")

    while True:
        try:
            user_input = input(": ").strip()
            if not user_input:
                continue

            if user_input.lower() in ("/exit", "exit", "quit"):
                print("Goodbye!")
                break

            if user_input == "/c":
                print("\033[2J\033[H", end="")
                continue

            if user_input == "/ch":
                chat_history.clear_history()
                print("Chat history cleared.")
                continue

            if user_input == "/h":
                if len(chat_history) == 0:
                    print("No chat history.")
                else:
                    for idx, message in enumerate(chat_history):
                        print(f"[{idx+1}] {message['role'].upper()}:\n{message['content']}\n{'-'*40}")
                continue

            chat_history.add_user_message(user_input)

            print("\n Determining best search method...\n")
            search_method = chat_agent.decide_search_method(user_input)
            if search_method not in ("vector", "sql", "hybrid"):
                search_method = "sql"
            print(" LLM has chosen the search method:", search_method.upper(), "\n")

            if search_method == "vector":
                print(" Running VECTOR search...\n")
                columns, rows = chat_agent.run_vector_search(db, user_input)
                chat_history.add_assistant_message(f"Vector search results:\n{rows}")

                # Generate natural language summary
                final_answer = chat_agent.generate_final_response(
                    user_question=user_input,
                    vector_results=(columns, rows),
                    sql_results=None
                )
                print(" Natural language answer:\n" + "-" * 60)
                print(final_answer)
                print("-" * 60 + "\n")
                continue

            print(" Agent 1 (Domain Expert) is analyzing your query...\n")
            agent1_response, tables = chat_agent.run_agent1(db, user_input, chat_history.get_history())

            print(" Agent 1 Plan:\n" + "-" * 60)
            print(agent1_response)
            print("-" * 60 + "\n")

            if not tables:
                print("Agent 1 could not identify relevant tables.")
                continue

            print(" Tables selected by Agent 1:", ", ".join(tables) + "\n")

            if search_method == "sql":
                print(" Running SQL search for precise results...\n")
                sql_query = chat_agent.run_agent2(
                    db=db,
                    user_question=user_input,
                    agent1_plan=agent1_response,
                    selected_tables=tables,
                    chat_history=chat_history.get_history()
                )
                sql_query = clean_sql_query(sql_query)

                print(" Generated SQL Query:\n" + "-" * 60)
                print(sql_query)
                print("-" * 60 + "\n")

                columns, rows = execute_sql_query(conn_str, sql_query)
                chat_history.add_assistant_message(f"SQL Query:\n{sql_query}\nResults:\n{rows}")

                # Natural language summary from SQL results only
                final_answer = chat_agent.generate_final_response(
                    user_question=user_input,
                    vector_results=None,
                    sql_results=(columns, rows)
                )
                print(" Natural language answer:\n" + "-" * 60)
                print(final_answer)
                print("-" * 60 + "\n")

            else:  # HYBRID
                print(" Running HYBRID search...\n")
                columns_vec, rows_vec = chat_agent.run_vector_search(db, user_input, top=1)
                chat_history.add_assistant_message(f"Vector search results:\n{rows_vec}")

                columns, rows = chat_agent.run_hybrid_search(
                    db=db,
                    user_question=user_input,
                    agent1_plan=agent1_response,
                    selected_tables=tables,
                    chat_history=chat_history.get_history()
                )

                sql_query = chat_agent.run_agent2(
                    db=db,
                    user_question=user_input,
                    agent1_plan=agent1_response,
                    selected_tables=tables,
                    product_ids_filter=None,
                    chat_history=chat_history.get_history()
                )
                sql_query = clean_sql_query(sql_query)

                chat_history.add_assistant_message(
                    f"Hybrid SQL:\n{sql_query}\nResults:\n{rows}"
                )

                # Generate combined natural language summary using both vector and SQL results
                final_answer = chat_agent.generate_final_response(
                    user_question=user_input,
                    vector_results=(columns_vec, rows_vec),
                    sql_results=(columns, rows)
                )
                print(" Natural language answer:\n" + "-" * 60)
                print(final_answer)
                print("-" * 60 + "\n")

        except KeyboardInterrupt:
            print("\nInterrupted by user. Exiting.")
            sys.exit(0)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
