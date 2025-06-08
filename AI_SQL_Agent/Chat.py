from datetime import datetime
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
import re

class ChatAgent:
    def __init__(self):
        self.llm = ChatOllama(model="qwen2.5-coder:7b", temperature=0)
        self.mistral_llm = ChatOllama(model="llama3.2:3b", temperature=0)

    def _parse_llm_response(self, result):
        if isinstance(result, dict) and "text" in result:
            return result["text"].strip()
        elif hasattr(result, "content"):
            return result.content.strip()
        elif isinstance(result, str):
            return result.strip()
        else:
            return str(result).strip()

    def call_llm(self, prompt, user_input="", chat_history=None, model="qwen"):
        messages = [("system", prompt)]
        if chat_history:
            for msg in chat_history:
                messages.append((msg["role"], msg["content"]))
        if user_input:
            messages.append(("human", user_input))

        prompt_template = ChatPromptTemplate.from_messages(messages)
        selected_llm = self.llm if model == "qwen" else self.mistral_llm
        chain = prompt_template | selected_llm
        result = chain.invoke({"input": user_input})
        return self._parse_llm_response(result)

    def decide_search_method(self, user_question, chat_history=None):
        prompt = f"""You are a smart AI assistant that decides how to route user questions.

Choose only one method:
- **vector** → when the question is about product suitability, product recommendation, meaning, or product descriptions. It's always product centric.
- **sql** → when the question asks for exact facts, numbers, prices, stock, or structured info **without needing interpretation**.
- **hybrid** → when the question involves understanding product meaning (semantic) **and** structured logic like sales, price, stock, or filters.

Here are examples:
- "What's the best bike for mountains?" → vector
- "How many employees are there?" → sql
- "How many sales does the best bike for mountains have?" → hybrid
- "Which affordable electric bikes are in stock?" → hybrid
- "What is the price of product ID 123?" → sql

Now classify this user question:
\"\"\"{user_question}\"\"\"

Reply only with one of: vector, sql, hybrid."""
        return self.call_llm(prompt, user_input=user_question, chat_history=chat_history, model="mistral")

    def is_db_related(self, user_question):
        prompt = f"""You are an expert at determining if a question requires querying a structured SQL Server database.

Reply ONLY "YES" if it requires database access; reply ONLY "NO" if it is subjective or not based on structured data.

User question:
\"\"\"{user_question}\"\"\""""
        response = self.call_llm(prompt, user_input=user_question, model="mistral")
        return "yes" in response.lower()

    def agent1_prompt(self, table_descriptions, user_question, chat_history, search_method):
        hybrid_note = ""
        if search_method == "hybrid":
            hybrid_note = """
Note: A separate system will determine the most relevant ProductID(s) or row identifiers using semantic search (vector embeddings).
Do not try to identify the 'best' product or item yourself.
Instead, assume the relevant ProductID(s) will be provided to the SQL layer externally.
Write your plan accordingly and use placeholders like `@ProductIDs` or `IN (...)` where needed.
"""

        return f"""
You are an AI assistant that helps users query a SQL Server database. The tables, with descriptions, are:

{table_descriptions}

Today is {datetime.now():%Y-%m-%d}.

Your job:
- List relevant tables (max 5).
- Explain what to do.
- Do NOT generate SQL.
- Focus on SELECT-only logic.
- Be concise, formal, and break complex tasks into steps.
{hybrid_note}

User question:
{user_question}
"""

    def agent2_prompt(self, detailed_schema, user_question, agent1_plan=None, selected_tables=None, product_ids_filter=None):
        product_clause = ""
        if product_ids_filter:
            product_clause = f"""
    IMPORTANT: The relevant ProductIDs for this query are: {product_ids_filter}.
    You MUST include the filter:
    WHERE ProductID IN ({product_ids_filter})
    in the SQL query. 
    Do NOT use placeholders or comments. Use these exact ProductIDs in the WHERE clause.
    """

        return f"""
    You are a senior T-SQL expert. Write a single valid **SQL Server SELECT** query using only the provided schema.

    Schema:
    {detailed_schema}

    Plan from domain expert:
    {agent1_plan}

    User question:
    {user_question}
    {product_clause}

    STRICT RULES:
    - NO comments or placeholders.
    - Use exact ProductIDs provided above.
    - ONLY use column names explicitly shown.
    - JOIN only when necessary.
    - Use T-SQL syntax.
    """


    def run_vector_search(self, db, user_question, top=1, stock=100):
        conn = db.get_connection()
        cursor = conn.cursor()
        sql = "EXEC dbo.find_relevant_products_vector_search @prompt=?, @stock=?, @top=?"
        cursor.execute(sql, (user_question, stock, top))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return columns, rows

    def run_agent1(self, db, user_question, chat_history, search_method):
        # Fetch relevant table descriptions (you may want to filter this in db logic)
        table_desc = db.fetch_table_descriptions()

        # Build Agent 1 prompt
        prompt = self.agent1_prompt(
            table_descriptions=table_desc,
            user_question=user_question,
            chat_history=chat_history,
            search_method=search_method
        )

        print("\nAgent 1 (Domain Expert) is analyzing your query...\n")

        # Get response from LLM
        response = self.call_llm(prompt, user_input=user_question, chat_history=chat_history)

        # Extract [schema].[table] references, accounting for optional brackets
        tables = re.findall(r"\b(?:\[\s*)?([A-Za-z0-9_]+\.[A-Za-z0-9_]+)(?:\s*\])?\b", response)

        # Deduplicate table names
        return response.strip(), sorted(set(tables))

    def run_agent2(self, db, user_question, agent1_plan, selected_tables, product_ids_filter=None, chat_history=None):
        detailed_schema = db.fetch_table_schema(selected_tables)
        if not detailed_schema.strip():
            raise ValueError("Schema is empty. Check selected tables.")

        prompt = self.agent2_prompt(
            detailed_schema=detailed_schema,
            user_question=user_question,
            agent1_plan=agent1_plan,
            selected_tables=selected_tables,
            product_ids_filter=product_ids_filter
        )
        sql_query = self.call_llm(prompt, user_input=user_question, chat_history=chat_history)
        return sql_query.strip()

    def run_hybrid_search(self, db, user_question, agent1_plan, selected_tables, chat_history=None):
        columns_vec, rows_vec = self.run_vector_search(db, user_question, top=10)
        product_ids_str = ""
        try:
            productid_index = columns_vec.index("ProductID")
            product_ids = [str(row[productid_index]) for row in rows_vec]
            product_ids_str = ",".join(product_ids)
        except ValueError:
            pass

        # Pass the real product IDs here
        sql_query = self.run_agent2(
            db=db,
            user_question=user_question,
            agent1_plan=agent1_plan,
            selected_tables=selected_tables,
            product_ids_filter=product_ids_str or None,
            chat_history=chat_history
        )

        # No need for post-processing — the SQL must already include real ProductIDs
        print("Generated SQL Query (HYBRID):")
        print(sql_query)

        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return columns, rows

    
    def generate_final_response(self, user_question, vector_results=None, sql_results=None, model="qwen"):
        prompt = f"""You are a helpful assistant. Use the results of a product search to answer the user's question clearly and naturally.

User Question:
\"\"\"{user_question}\"\"\"

Vector Search Results:
{self._format_results(vector_results) if vector_results else "None"}

SQL Results:
{self._format_results(sql_results) if sql_results else "None"}

Respond with a helpful, concise summary based on these results."""
            
        return self.call_llm(prompt=prompt, model=model)
    
    def _format_results(self, results):
        if not results or len(results) != 2:
            return "No results."
        
        columns, rows = results
        formatted = []
        for row in rows:
            formatted.append(", ".join(f"{col}: {val}" for col, val in zip(columns, row)))
        return "\n".join(formatted[:5])  # Limit to top 5 rows
