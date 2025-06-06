from datetime import datetime
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
import re


class ChatAgent:
    def __init__(self):
        self.llm = ChatOllama(model="codellama:7b", temperature=0)

    def call_llm(self, prompt, user_input=""):
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", prompt),
            ("human", "{input}")
        ])
        chain = prompt_template | self.llm
        result = chain.invoke({"input": user_input})

        if isinstance(result, dict) and "text" in result:
            return result["text"].strip()
        elif hasattr(result, "content"):
            return result.content.strip()
        elif isinstance(result, str):
            return result.strip()
        else:
            return str(result).strip()

    def is_db_related(self, user_question):
        prompt = f"""
    You are an expert assistant who determines if a user question requires querying a structured SQL database containing info on products, sales, people and human resources.

    Reply ONLY with "YES" if the question requires looking up or querying data from the database.
    Reply ONLY with "NO" if the question is general, subjective, or unrelated to database facts.

    User question:
    \"\"\"{user_question}\"\"\"
    """
        response = self.call_llm(prompt).lower()
        if "yes" in response:
            return True
        return False

    
    def decide_search_method(self, user_question):
        """Determines whether to use 'sql', 'vector', or 'hybrid' search based on user intent."""
        prompt = f"""You are an expert assistant deciding how to answer user queries by selecting the best search method.

- Use 'vector' if the question is mainly about product meanings, descriptions, or suitability.
- Use 'sql' if the question is about exact numeric facts, counts, or lists from structured data.
- Use 'hybrid' if the question requires both semantic understanding and precise data retrieval, such as when a question involves product descriptions AND sales numbers.

Example queries:
- "Which product is best for mountain biking?" -> vector
- "How many sales were there last month?" -> sql
- "How many sales were there for the product best used for mountain biking?" -> hybrid

Now, given this user query:

\"\"\"{user_question}\"\"\"

Reply only with one of these words: vector, sql, or hybrid."""
        return self.call_llm(prompt)

    def agent1_prompt(self, table_descriptions, user_question):
        return f"""
You are an AI assistant that helps users to query the database. The tables in the database, with the related description, are:

{table_descriptions}

Use a professional tone when answering and provide a summary of data instead of lists.
If users ask about topics you don't know, answer that you don't know. Today's date is {datetime.now():%Y-%m-%d}.
You must answer providing a list of tables that must be used to answer the question and an explanation of what you'll be doing to answer the question.
You must use the provided tool to query the database.
If the request is complex, break it down into smaller steps and call the plugin as many times as needed. Ideally don't use more than 5 tables in the same query.
You must only create read logic.

IMPORTANT
-Do not write the query, only provide table names and a query semantics.

User question:
{user_question}
"""

    def agent2_prompt(self, detailed_schema, user_question, agent1_plan=None, selected_tables=None, product_ids_filter=None):
        product_id_clause = ""
        if product_ids_filter:
            product_id_clause = f"""
    IMPORTANT:
    - if a vector search has been invoked You MUST filter results using this list of relevant ProductIDs from a semantic (vector) search: ({product_ids_filter})
    - Only include results WHERE [Production].[Product].[ProductID] IN ({product_ids_filter})
    - If the selected tables don't include [Production].[Product], use the appropriate table containing ProductID as per the schema."""

        return f"""
    You are a senior SQL Server expert tasked with writing production-quality T-SQL queries using the schema provided with a syntax for sql server only !

    Provided information:
    1. SCHEMA: {detailed_schema}
    2. AGENT 1 PLAN: {agent1_plan}
    3. USER QUESTION: {user_question}
    {product_id_clause}

    STRICT INSTRUCTIONS:
    - ONLY generate a **single raw SQL SELECT statement**.
    - ALWAYS use column and table names that appear *exactly* in the provided schema — do NOT assume or guess any columns.
    - NEVER use columns like `Name`, `CategoryID`, etc., unless they are explicitly listed in the schema for that table.
    - If a field like `CustomerName` or `ProductName` is needed, derive it via a JOIN if the schema shows the source.
    - DO NOT use markdown, comments, or aliases for nonexistent columns.

    - Apply the WHERE filter on ProductID when provided.
    - Join only necessary tables based on Agent 1's plan.


    Generate only valid SQL.
    """

    def run_vector_search(self, db, user_question, top=1, stock=100):
        conn = db.get_connection()
        cursor = conn.cursor()

        # Prepare the SQL command string with placeholders
        sql = "EXEC dbo.find_relevant_products_vector_search @prompt=?, @stock=?, @top=?"

        # Print out the command and parameters for debugging
        print(f"DEBUG: Executing SQL:\n{sql}")
        print(f"DEBUG: With parameters:\nPrompt: {user_question}\nStock: {stock}\nTop: {top}")

        cursor.execute(sql, (user_question, stock, top))

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return columns, rows

    def run_agent1(self, db, user_question):
        """Calls Agent 1 to analyze the user query and identify relevant tables."""
        table_desc = db.fetch_table_descriptions()
        prompt = self.agent1_prompt(table_desc, user_question)
        response = self.call_llm(prompt)

        # Extract table names using regex like [Schema].[Table] or Schema.Table
        tables = list(set(re.findall(r"\[?([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\]?", response)))
        return response, tables

    def run_agent2(self, db, user_question, agent1_plan, selected_tables, product_ids_filter=None):
        """Calls Agent 2 to generate a SQL query based on Agent 1's plan and user question."""
        detailed_schema = db.fetch_table_schema(selected_tables)
        prompt = self.agent2_prompt(
            detailed_schema=detailed_schema,
            user_question=user_question,
            agent1_plan=agent1_plan,
            selected_tables=selected_tables,
            product_ids_filter=product_ids_filter
        )
        sql_query = self.call_llm(prompt)
        return sql_query.strip()

    def run_hybrid_search(self, db, user_question, agent1_plan, selected_tables):
        # Step 1: Vector search
        columns_vec, rows_vec = self.run_vector_search(db, user_question, top=1)

        product_ids_str = ""
        try:
            productid_index = columns_vec.index("ProductID")
            product_ids = [str(row[productid_index]) for row in rows_vec]
            product_ids_str = ",".join(product_ids)
        except ValueError:
            pass  # If ProductID not found, skip filtering

        # Step 2: Generate SQL query string via Agent 2 with filter
        sql_query = self.run_agent2(
            db=db,
            user_question=user_question,
            agent1_plan=agent1_plan,
            selected_tables=selected_tables,
            product_ids_filter=product_ids_str or None
        )

        # Step 3: Execute generated SQL query and fetch results
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return columns, rows


