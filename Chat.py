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

    def call_llm(self, prompt, user_input="", chat_history=None, model="llama3.2:3b"):
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
        prompt = f"""You are an AI assistant that selects the best method to answer a user’s question using the following options:

        - **vector**: Use ONLY if the question is subjective, preference-based, or asks for product recommendations, meaning, or similarity (e.g., "best shoes for hiking").
        - **sql**: Use for all fact-based queries involving customers, sales, prices, IDs, quantities, dates, inventory, or performance metrics.
        - **hybrid**: Use ONLY when the question mixes subjective/product-suitability filtering *and* factual constraints (e.g., "Which product with high traction sold the most last year?").

        Examples:
        - "What's the total revenue by country?" → sql
        - "Recommend a helmet for downhill biking" → vector
        - "Which of the most suitable helmets had the highest sales?" → hybrid

        User question:
        \"\"\"{user_question}\"\"\"

        Reply with one word only: **sql**, **vector**, or **hybrid**."""

        return self.call_llm(prompt, user_input=user_question, chat_history=chat_history, model="llama3.2:3b")


    def is_db_related(self, user_question):
        prompt = f"""You are an expert at determining if a question requires querying a structured SQL Server database.

Reply ONLY "YES" if it requires database access; reply ONLY "NO" if it is subjective or not based on structured data.

User question:
\"\"\"{user_question}\"\"\""""
        response = self.call_llm(prompt, user_input=user_question, model="mistral")
        return "yes" in response.lower()

    def agent1_prompt(self, table_descriptions, user_question, chat_history, search_method="sql"):
        method_desc = {
            "vector": "Focus on tables relevant for semantic product suitability or recommendations.",
            "sql": "Focus on tables for precise, fact-based data retrieval.",
            "hybrid": (
                "Combine tables for semantic product suitability AND tables for precise filtering or "
                "numeric fact retrieval. Plan should address both aspects."
            )
        }

        return f"""
    You are an AI assistant helping users query a SQL Server database.

    Tables available, with descriptions:
    {table_descriptions}

    Today is {datetime.now():%Y-%m-%d}.

    Search method: {search_method.upper()}
    Instruction: {method_desc.get(search_method, 'Focus on relevant tables for the query.')}

    Your job:
    - List relevant tables (max 5).
    - Explain your approach to combine semantic search and precise filtering if hybrid.
    - Do NOT generate SQL.
    - Focus on SELECT-only logic.
    - Be concise, formal, and break complex tasks into steps.

    User question:
    {user_question}
    """


    def agent2_prompt(self, detailed_schema, user_question, agent1_plan=None, selected_tables=None, product_ids_filter=None):
        product_clause = ""
        if product_ids_filter:
            product_clause = f"""
    STRICT REQUIREMENT:

    You MUST use the following ProductID list to filter results:

    WHERE ProductID IN ({product_ids_filter})
    -- DO NOT use LIKE, Name, or Category filters instead.


    - This clause MUST appear in the final SQL.
    - DO NOT replace this with any other filter (e.g., LIKE, descriptions, or category).
    - DO NOT skip, modify, or ignore this clause.
    - Failure to include it = INVALID SQL.
    """

        return f"""
    You are a senior T-SQL expert. Write one valid **SQL Server SELECT** query using only the provided schema and requirements.

    Schema:
    {detailed_schema}

    Plan from domain expert:
    {agent1_plan}

    User question:
    {user_question}

    {product_clause}

    ADDITIONAL RULES:
    - Use ONLY the columns and tables from the schema above.
    - Use JOINs only when needed.
    - Use standard T-SQL syntax.
    - NO comments, no placeholders.
    - Output ONLY the query.
    """

    def run_vector_search(self, db, user_question, top=1, stock=100):
        results = db.vector_search_products(prompt=user_question, stock=stock, top=top)
        if not results:
            return [], []

        columns = list(results[0].keys())
        rows = [tuple(row[col] for col in columns) for row in results]
        return columns, rows


    def run_agent1(self, db, user_question, chat_history, search_method="sql"):
        table_desc = db.fetch_table_descriptions()
        prompt = self.agent1_prompt(table_desc, user_question, chat_history, search_method)
        response = self.call_llm(prompt, user_input=user_question, chat_history=chat_history)
        tables = re.findall(r"\[?([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\]?", response)
        return response, list(set(tables))



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
        columns_vec, rows_vec = self.run_vector_search(db, user_question, top=1)

        # Print vector search results before proceeding to SQL
        print("Vector Search Results:")
        print("-" * 60)
        if not rows_vec:
            print("No results found from vector search.")
        else:
            for row in rows_vec[:1]:  # Show top 5 results for brevity
                print(", ".join(f"{col}: {val}" for col, val in zip(columns_vec, row)))
        print("-" * 60 + "\n")
        product_ids_str = ""
        try:
            productid_index = columns_vec.index("ProductID")
            product_ids = [str(row[productid_index]) for row in rows_vec]
            product_ids_str = ",".join(product_ids)
        except ValueError:
            pass

        sql_query = self.run_agent2(
            db=db,
            user_question=user_question,
            agent1_plan=agent1_plan,
            selected_tables=selected_tables,
            product_ids_filter=product_ids_str or None,
            chat_history=chat_history
        )

        print("Generated SQL Query (HYBRID):")
        print(sql_query)

        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return sql_query, columns, rows



    
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


