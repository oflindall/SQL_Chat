import pyodbc

class Database:
    def __init__(self, conn_str):
        self.conn_str = conn_str

    def get_connection(self):
        return pyodbc.connect(self.conn_str, autocommit=True)

    def fetch_table_descriptions(self):
        table_descriptions = {
            "dbo.AWBuildVersion": "Stores versioning information for the AdventureWorks sample database.",
            "dbo.DatabaseLog": "Captures change tracking information for auditing database-level events.",
            "dbo.ErrorLog": "Logs error messages raised by the database system or application.",
            "HumanResources.Department": "Stores information about company departments including name and group.",
            "HumanResources.Employee": "Contains personal and organizational information about employees.",
            "HumanResources.EmployeeDepartmentHistory": "Tracks the historical assignments of employees to departments and shifts.",
            "HumanResources.EmployeePayHistory": "Records changes to employee pay rates over time.",
            "HumanResources.JobCandidate": "Stores resumes and job application data submitted by potential employees.",
            "HumanResources.Shift": "Defines work shifts, including start and end times.",
            "Person.Address": "Stores physical addresses used by employees, customers, or vendors.",
            "Person.AddressType": "Defines types of addresses such as billing or shipping.",
            "Person.BusinessEntity": "Central entity used to unify persons, vendors, and stores.",
            "Person.BusinessEntityAddress": "Links business entities to their addresses.",
            "Person.BusinessEntityContact": "Stores contact relationships between people and business entities.",
            "Person.ContactType": "Defines roles for contacts such as 'Purchasing Manager'.",
            "Person.CountryRegion": "Stores country or region names used for address and sales data.",
            "Person.EmailAddress": "Stores email addresses and links them to people.",
            "Person.Password": "Contains password hashes and security-related metadata for users.",
            "Person.Person": "Stores contact information for all individuals including employees and customers.",
            "Person.PersonPhone": "Links phone numbers to people along with the phone type.",
            "Person.PhoneNumberType": "Defines types of phone numbers (e.g., Mobile, Home, Work).",
            "Person.StateProvince": "Contains state and province data tied to countries/regions.",
            "Production.BillOfMaterials": "Defines how products are assembled from components.",
            "Production.Culture": "Stores culture identifiers used for product model translations.",
            "Production.Document": "Stores product-related documents such as manuals and specs.",
            "Production.Illustration": "Contains drawings related to product models.",
            "Production.Location": "Tracks production locations within a manufacturing plant.",
            "Production.Product": "Stores product metadata including name, number, and manufacturing info.",
            "Production.ProductCategory": "Defines top-level product categorization.",
            "Production.ProductCostHistory": "Tracks historical standard costs of products.",
            "Production.ProductDescription": "Contains textual product descriptions used in sales literature.",
            "Production.ProductDescriptionEmbeddings": "Vector embeddings of product descriptions for semantic search and AI use cases.",
            "Production.ProductDocument": "Links products to their related documents.",
            "Production.ProductInventory": "Tracks inventory levels by product and location.",
            "Production.ProductListPriceHistory": "Stores historical list prices for products.",
            "Production.ProductModel": "Defines product modeling metadata and structure.",
            "Production.ProductModelIllustration": "Links product models to illustrative content.",
            "Production.ProductModelProductDescriptionCulture": "Supports multilingual product descriptions by linking models and descriptions with culture.",
            "Production.ProductPhoto": "Stores product images including thumbnails.",
            "Production.ProductProductPhoto": "Associates products with their photos.",
            "Production.ProductReview": "Captures customer reviews and ratings for products.",
            "Production.ProductSubcategory": "Defines second-level categorization of products.",
            "Production.ScrapReason": "Stores reasons for scrapping a product during manufacturing.",
            "Production.TransactionHistory": "Records detailed transactions for inventory movements.",
            "Production.TransactionHistoryArchive": "Archived transaction history data for long-term retention.",
            "Production.UnitMeasure": "Defines units of measure used in manufacturing and purchasing.",
            "Production.WorkOrder": "Details work orders for product assembly operations.",
            "Production.WorkOrderRouting": "Specifies the steps and resource routing for work orders.",
            "Purchasing.ProductVendor": "Links vendors to the products they supply.",
            "Purchasing.PurchaseOrderDetail": "Stores individual line items of purchase orders.",
            "Purchasing.PurchaseOrderHeader": "Contains header information for purchase orders, including vendor and order dates.",
            "Purchasing.ShipMethod": "Lists available shipping methods and rates.",
            "Purchasing.Vendor": "Stores vendor account information and purchasing metadata.",
            "Sales.CountryRegionCurrency": "Links countries/regions with their supported currencies.",
            "Sales.CreditCard": "Stores credit card information used in customer orders.",
            "Sales.Currency": "Stores ISO currency codes used in sales and purchasing.",
            "Sales.CurrencyRate": "Tracks exchange rates between currencies.",
            "Sales.Customer": "Stores customer account information, linking to person or store.",
            "Sales.PersonCreditCard": "Links people to their credit cards for sales transactions.",
            "Sales.SalesOrderDetail": "Line item details for sales orders.",
            "Sales.SalesOrderHeader": "Header information for sales orders, including billing, shipping, and totals.",
            "Sales.SalesOrderHeaderSalesReason": "Links sales orders with the reasons that influenced the sale.",
            "Sales.SalesPerson": "Stores data about employees assigned as sales representatives.",
            "Sales.SalesPersonQuotaHistory": "Tracks sales quotas assigned to salespeople over time.",
            "Sales.SalesReason": "Lists predefined reasons that influence a sale.",
            "Sales.SalesTaxRate": "Defines sales tax rates based on geography and tax type.",
            "Sales.SalesTerritory": "Defines geographic sales regions and associated metadata.",
            "Sales.SalesTerritoryHistory": "Tracks historical assignments of salespeople to territories.",
            "Sales.ShoppingCartItem": "Stores items placed in a customer’s online shopping cart.",
            "Sales.SpecialOffer": "Stores promotional discounts available on products.",
            "Sales.SpecialOfferProduct": "Links special offers to applicable products.",
            "Sales.Store": "Stores retail customer information including demographics."
        }

        return "\n".join(f"[{k}]: {v}" for k, v in table_descriptions.items())

    def fetch_table_schema(self, tables):
        placeholders = ",".join("?" for _ in tables)
        query = f"""
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                c.name AS column_name,
                ty.name AS data_type,
                c.max_length,
                c.is_nullable,
                ep.value AS column_description
            FROM 
                sys.tables t
            INNER JOIN 
                sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN 
                sys.columns c ON t.object_id = c.object_id
            INNER JOIN 
                sys.types ty ON c.user_type_id = ty.user_type_id
            LEFT JOIN 
                sys.extended_properties ep ON t.object_id = ep.major_id 
                    AND c.column_id = ep.minor_id
                    AND ep.name = 'MS_Description'
            WHERE 
                s.name + '.' + t.name IN ({placeholders})
            ORDER BY 
                s.name, t.name, c.column_id;
        """

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, tables)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        schema_map = {}
        for row in rows:
            key = f"[{row.schema_name}].[{row.table_name}]"
            desc = row.column_description if row.column_description else ""
            nullable = "NULL" if row.is_nullable else "NOT NULL"
            col_def = f"{row.column_name} ({row.data_type}) -- {desc} {nullable}".strip()
            if key not in schema_map:
                schema_map[key] = []
            schema_map[key].append(col_def)

        schema_str = ""
        for table, cols in schema_map.items():
            schema_str += f"{table}:\n"
            schema_str += "\n".join(cols)
            schema_str += "\n\n"

        return schema_str

    def vector_search_products(self, prompt, stock=100, top=1):
        query = """
            EXEC dbo.find_relevant_products_vector_search_oli 
                @prompt = ?, 
                @stock = ?, 
                @top = ?;
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (prompt, stock, top))
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return results
