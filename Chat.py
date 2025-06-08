from datetime import datetime
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
import re

class ChatAgent:
    def __init__(self):
        self.llm = ChatOllama(model="qwen2.5-coder:7b", temperature=0)
        self.mistral_llm = ChatOllama(model="gemma:2b-instruct", temperature=0)

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

Choose from exactly one of:
- vector → for vague, descriptive, or suitability-based queries about products
- sql → for precise, factual, or count-based queries from structured data
- hybrid → when both types of understanding are needed

User question:
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

    def agent1_prompt(self, table_descriptions, user_question, chat_history):
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

User question:
{user_question}
"""

    def agent2_prompt(self, detailed_schema, user_question, agent1_plan=None, selected_tables=None, product_ids_filter=None):
        product_clause = ""
        if product_ids_filter:
            product_clause = f"""
- Use WHERE ProductID IN ({product_ids_filter}) if relevant.
- If ProductID is in another table, adapt accordingly."""

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
- NO comments, markdown, aliases for unknown columns.
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

    def run_agent1(self, db, user_question, chat_history):
        table_desc = db.fetch_table_descriptions()
        prompt = self.agent1_prompt(table_desc, user_question, chat_history)  # <-- pass chat_history here
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

        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return columns, rows
