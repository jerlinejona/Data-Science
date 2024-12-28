import psycopg2
import pandas as pd
import streamlit as st

# Database connection details
host = 'localhost'
database = 'Project'
user = 'postgres'
password = 'kenny'

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()
    st.success("Connected to the PostgreSQL database successfully!")
except Exception as e:
    st.error(f"Failed to connect to the database: {e}")

# Define the queries
queries = {
    "Top 10 highest revenue-generating products": """
        SELECT "product_id", "category" 
        FROM orderss 
        ORDER BY "profit" DESC 
        LIMIT 10;
    """,
    "Top 5 cities with the highest profit margins": """
        SELECT "city", "profit" 
        FROM orderss 
        ORDER BY "profit" DESC 
        LIMIT 5;
    """,
    "Total discount given for each category": """
        SELECT category, SUM(discount) AS total_discount
        FROM orderss 
        GROUP BY category 
        ORDER BY total_discount DESC;
    """,
    "Average sale price per product category": """
        SELECT category, AVG(sale_price) AS avg_sale_price
        FROM orderss 
        GROUP BY category 
        ORDER BY avg_sale_price DESC;
    """,
    "The region with the highest average sale price": """
        SELECT region, AVG(sale_price) AS avg_sale_price
        FROM orderss 
        GROUP BY region 
        ORDER BY avg_sale_price DESC 
        LIMIT 1;
    """,
    "Total profit per category": """
        SELECT category, SUM(profit) AS total_profit
        FROM orderss 
        GROUP BY category 
        ORDER BY total_profit DESC;
    """,
    "Top 3 segments with the highest quantity of orders": """
        SELECT segment, SUM(quantity) AS total_quantity
        FROM orderss  
        WHERE quantity > 0 AND segment IS NOT NULL
        GROUP BY segment 
        ORDER BY total_quantity DESC 
        LIMIT 3;
    """,
    "Average discount percent given per region": """
        SELECT region,
        AVG((cost_price - sale_price) / cost_price) * 100 AS avg_discount_percentage
        FROM orderss
        WHERE cost_price > 0 AND sale_price > 0
        GROUP BY region 
        ORDER BY avg_discount_percentage DESC;
    """,
    "The product category with the highest total profit": """
        SELECT category,
        SUM(sale_price - cost_price) AS total_profit
        FROM orderss
        GROUP BY category
        ORDER BY total_profit DESC
        LIMIT 1;
    """,
    "Total revenue generated per year": """
        SELECT EXTRACT(YEAR FROM order_date::DATE) AS year,
        SUM(sale_price) AS total_revenue
        FROM orderss
        GROUP BY year
        ORDER BY year;
    """,
    "Total order details along with product pricing": """
        SELECT o1.order_id, o1.order_date, o1.city, o2.cost_price, o2.list_price
        FROM order1 o1
        INNER JOIN order2 o2 ON o1.product_id = o2.product_id LIMIT 5;
    """,
    "Total orders with profitable products": """ 
        SELECT o1.order_id, o1.state, o2.product_id, o2.profit
        FROM order1 o1
        INNER JOIN order2 o2 ON o1.product_id = o2.product_id
        WHERE o2.profit > 0;
    """,
    "List Products with High Profit in a Specific Region": """
        SELECT o1.product_id, o1.region, o2.profit
        FROM order1 o1
        INNER JOIN order2 o2 ON o1.product_id = o2.product_id
        WHERE o1.region = 'West' AND o2.profit > 1000; 
    """,
    "Orders Without Matching Product Pricing": """     
        SELECT o1.order_id, o1.order_date, o1.segment, o2.quantity, o2.sale_price
        FROM order1 o1
        LEFT JOIN order2 o2 ON o1.product_id = o2.product_id;
    """,
    "Find Orders with Discounted Products": """
        SELECT o1.order_id, o1.region, o2.product_id, o2.discount_percent
        FROM order1 o1
        INNER JOIN order2 o2 ON o1.product_id = o2.product_id
        WHERE o2.discount_percent > 0;
    """,
    "Find Most Popular Shipping Mode for Discounted Products": """
        SELECT o1.ship_mode, COUNT(o1.order_id) AS order_count
        FROM order1 o1
        INNER JOIN order2 o2 ON o1.product_id = o2.product_id
        WHERE o2.discount_percent > 0
        GROUP BY o1.ship_mode
        ORDER BY order_count DESC;
    """,
    "Products Without Matching Orders": """
        SELECT o1.order_id, o2.product_id, o2.cost_price, o2.profit
        FROM order1 o1
        RIGHT JOIN order2 o2 ON o1.product_id = o2.product_id;  
    """,
    "Calculate Total Profit by Region": """   
        SELECT o1.region, SUM(o2.profit) AS total_profit
        FROM order1 o1
        INNER JOIN order2 o2 ON o1.product_id = o2.product_id
        GROUP BY o1.region; 
    """,
    "Product Sales by Sub-Category": """   
        SELECT o1.sub_category, SUM(o2.sale_price * o2.quantity) AS total_sales
        FROM order1 o1
        INNER JOIN order2 o2 ON o1.product_id = o2.product_id
        GROUP BY o1.sub_category;  
    """,
    "Include All Orders and Products": """ 
        SELECT o1.order_id, o1.city, o2.product_id, o2.sale_price
        FROM order1 o1
        FULL OUTER JOIN order2 o2 ON o1.product_id = o2.product_id;  
    """            
}

# Streamlit Dropdown
st.title("Retail Order Data Analysis")
selected_query = st.selectbox(
    "Select a query to execute",
    list(queries.keys())
)

if st.button("Execute Query"):
    # Execute the selected query
    query = queries[selected_query]
    try:
        cursor.execute(query)
        results = cursor.fetchall()

        # Define column names based on your query
        column_names = [desc[0] for desc in cursor.description]

        # Convert the results into a DataFrame
        df = pd.DataFrame(results, columns=column_names)

        # Display results
        st.subheader(selected_query)
        st.dataframe(df)

        # Visualize specific queries
        if "total_revenue" in df.columns:
            st.line_chart(df.set_index('year')['total_revenue'])
        elif "total_profit" in df.columns:
            st.bar_chart(df.set_index('category')['total_profit'])
        elif "avg_sale_price" in df.columns and "region" in df.columns:
            st.bar_chart(df.set_index('region')['avg_sale_price'])

    except Exception as e:
        st.error(f"Error executing query: {e}")

# Close the connection
cursor.close()
conn.close()