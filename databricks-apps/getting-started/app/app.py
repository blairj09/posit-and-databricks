import os
from shiny import App, ui, render, reactive
from shinywidgets import output_widget, render_widget  # Add shinywidgets import
import ibis
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from databricks.sdk.core import Config

# Load environment variables
load_dotenv()

# UI
app_ui = ui.page_fillable(
    ui.panel_title("Sales Dashboard"),
    
    # Summary cards row
    ui.row(
        ui.column(3, ui.value_box("Total Sales", ui.output_text("total_sales"), theme="primary")),
        ui.column(3, ui.value_box("Total Transactions", ui.output_text("total_transactions"), theme="info")),
        ui.column(3, ui.value_box("Avg Transaction", ui.output_text("avg_transaction"), theme="success")),
        ui.column(3, ui.value_box("Unique Customers", ui.output_text("unique_customers"), theme="warning"))
    ),
    
    ui.br(),
    
    # Filters row
    ui.row(
        ui.column(4, ui.input_selectize("region_filter", "Select Regions:", choices=[], multiple=True)),
        ui.column(4, ui.input_selectize("product_filter", "Select Products (Top 20):", choices=[], multiple=True)),
        ui.column(4, ui.input_date_range("date_filter", "Date Range:"))
    ),
    
    ui.br(),
    
    # Charts - Updated to use output_widget for Plotly charts
    ui.navset_tab(
        ui.nav_panel("Regional Analysis",
            ui.row(
                ui.column(6, output_widget("regional_sales_plot")),
                ui.column(6, output_widget("regional_metrics_plot"))
            ),
            ui.row(
                ui.column(12, output_widget("sales_timeline_plot"))
            )
        ),
        ui.nav_panel("Product Analysis", 
            ui.row(
                ui.column(6, output_widget("top_products_plot")),
                ui.column(6, output_widget("product_profitability_plot"))
            ),
            ui.row(
                ui.column(12, output_widget("product_region_heatmap"))
            )
        ),
        ui.nav_panel("Data Tables",
            ui.navset_tab(
                ui.nav_panel("Regional Summary", ui.output_data_frame("regional_table")),
                ui.nav_panel("Product Summary", ui.output_data_frame("product_table"))
            )
        )
    )
)

def server(input, output, session):
    
    @reactive.calc
    def get_connection():
        """Initialize Databricks connection"""
        try:
            # Get warehouse ID from environment variable  
            warehouse_id = os.getenv("WAREHOUSE_ID")
            if not warehouse_id:
                raise ValueError("WAREHOUSE_ID environment variable not set")
            
            # Initialize Databricks config
            cfg = Config()
            
            # Connect using the same pattern as the EDA notebook
            con = ibis.databricks.connect(
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                server_hostname=cfg.host,
                credentials_provider=lambda: cfg.authenticate,
                catalog=os.getenv("DATABRICKS_CATALOG", "jb-demos"),
                schema=os.getenv("DATABRICKS_SCHEMA", "sales_example"),
                memtable_volume=os.getenv("DATABRICKS_MEMTABLE_VOLUME", "ibis_memtable")
            )
            
            return con
        except Exception as e:
            print(f"Failed to connect to Databricks: {str(e)}")
            return None
    
    @reactive.calc
    def get_sales_data():
        """Get sales data from Databricks"""
        con = get_connection()
        if con is None:
            return pd.DataFrame()
        
        try:
            sales = con.table("sales")
            
            # Apply filters
            filtered_sales = sales
            
            # Date filter
            if input.date_filter():
                start_date, end_date = input.date_filter()
                filtered_sales = filtered_sales.filter(
                    (filtered_sales.date >= start_date) & 
                    (filtered_sales.date <= end_date)
                )
            
            # Region filter
            if input.region_filter():
                filtered_sales = filtered_sales.filter(
                    filtered_sales.region.isin(input.region_filter())
                )
            
            # Product filter
            if input.product_filter():
                filtered_sales = filtered_sales.filter(
                    filtered_sales.product.isin(input.product_filter())
                )
            
            return filtered_sales.execute()
        except Exception as e:
            print(f"Error fetching sales data: {str(e)}")
            return pd.DataFrame()
    
    @reactive.calc
    def get_filter_choices():
        """Get unique values for filters"""
        con = get_connection()
        if con is None:
            return {"regions": [], "products": [], "date_range": None}
        
        try:
            sales = con.table("sales")
            
            # Get unique regions
            regions = sales.select("region").distinct().execute()["region"].tolist()
            
            # Get top 20 products by sales
            top_products = (
                sales
                .group_by("product")
                .aggregate(total_sales=sales.total_amount.sum())
                .order_by(ibis.desc("total_sales"))
                .limit(20)
                .execute()
            )["product"].tolist()
            
            # Get date range
            date_stats = sales.select(
                min_date=sales.date.min(),
                max_date=sales.date.max()
            ).execute()
            
            return {
                "regions": regions,
                "products": top_products,
                "date_range": (date_stats["min_date"].iloc[0], date_stats["max_date"].iloc[0])
            }
        except Exception as e:
            print(f"Error getting filter choices: {str(e)}")
            return {"regions": [], "products": [], "date_range": None}
    
    # Update filter choices when app loads
    @reactive.effect
    def update_filters():
        choices = get_filter_choices()
        
        ui.update_selectize("region_filter", choices=choices["regions"], selected=choices["regions"])
        ui.update_selectize("product_filter", choices=choices["products"])
        
        if choices["date_range"]:
            ui.update_date_range("date_filter", 
                                start=choices["date_range"][0], 
                                end=choices["date_range"][1])
    
    # Summary metrics
    @render.text
    def total_sales():
        data = get_sales_data()
        if data.empty:
            return "$0"
        return f"${data['total_amount'].sum():,.0f}"
    
    @render.text
    def total_transactions():
        data = get_sales_data()
        if data.empty:
            return "0"
        return f"{len(data):,}"
    
    @render.text
    def avg_transaction():
        data = get_sales_data()
        if data.empty:
            return "$0"
        return f"${data['total_amount'].mean():.2f}"
    
    @render.text
    def unique_customers():
        data = get_sales_data()
        if data.empty:
            return "0"
        return f"{data['customer_id'].nunique():,}"
    # Regional Analysis Plots - Updated to use @render_widget
    @render_widget
    def regional_sales_plot():
        data = get_sales_data()
        if data.empty:
            return None
        
        regional_summary = (
            data.groupby("region")
            .agg({
                "total_amount": "sum",
                "customer_id": "count"
            })
            .reset_index()
            .sort_values("total_amount", ascending=True)
        )
        
        fig = px.bar(
            regional_summary,
            x="total_amount",
            y="region",
            orientation="h",
            title="Total Sales by Region",
            labels={"total_amount": "Total Sales ($)", "region": "Region"}
        )
        
        fig.update_layout(height=400)
        return fig
    
    @render_widget
    def regional_metrics_plot():
        data = get_sales_data()
        if data.empty:
            return None
        
        regional_metrics = (
            data.groupby("region")
            .agg({
                "total_amount": ["sum", "mean"],
                "customer_id": "nunique",
                "quantity": "sum"
            })
            .reset_index()
        )
        
        # Flatten column names
        regional_metrics.columns = ["region", "total_sales", "avg_transaction", "unique_customers", "total_quantity"]
        regional_metrics["sales_per_customer"] = regional_metrics["total_sales"] / regional_metrics["unique_customers"]
        
        fig = px.scatter(
            regional_metrics,
            x="unique_customers",
            y="sales_per_customer",
            size="total_sales",
            color="region",
            title="Sales per Customer vs Customer Count by Region",
            labels={
                "unique_customers": "Number of Customers",
                "sales_per_customer": "Sales per Customer ($)"
            }
        )
        
        fig.update_layout(height=400)
        return fig
    
    @render_widget
    def sales_timeline_plot():
        data = get_sales_data()
        if data.empty:
            return None
        
        # Convert date column to datetime
        data["date"] = pd.to_datetime(data["date"])
        
        # Group by month and region
        monthly_data = (
            data.groupby([data["date"].dt.to_period("M"), "region"])
            .agg({"total_amount": "sum"})
            .reset_index()
        )
        monthly_data["date"] = monthly_data["date"].dt.to_timestamp()
        
        fig = px.line(
            monthly_data,
            x="date",
            y="total_amount",
            color="region",
            title="Sales Trend Over Time by Region",
            labels={"total_amount": "Total Sales ($)", "date": "Date"}
        )
        
        fig.update_layout(height=400)
        return fig
    
    # Product Analysis Plots - Updated to use @render_widget
    @render_widget
    def top_products_plot():
        data = get_sales_data()
        if data.empty:
            return None
        
        top_products = (
            data.groupby("product")
            .agg({"total_amount": "sum"})
            .reset_index()
            .sort_values("total_amount", ascending=False)
            .head(10)
        )
        
        fig = px.bar(
            top_products,
            x="product",
            y="total_amount",
            title="Top 10 Products by Revenue",
            labels={"total_amount": "Total Sales ($)", "product": "Product"}
        )
        
        fig.update_xaxes(tickangle=45)
        fig.update_layout(height=400)
        return fig
    
    @render_widget
    def product_profitability_plot():
        data = get_sales_data()
        if data.empty:
            return None
        
        product_metrics = (
            data.groupby("product")
            .agg({
                "total_amount": "sum",
                "quantity": "sum",
                "unit_price": "mean",
                "discount_percent": "mean"
            })
            .reset_index()
        )
        
        product_metrics["revenue_per_unit"] = product_metrics["total_amount"] / product_metrics["quantity"]
        top_profitable = product_metrics.nlargest(15, "revenue_per_unit")
        
        fig = px.scatter(
            top_profitable,
            x="quantity",
            y="revenue_per_unit",
            size="total_amount",
            color="discount_percent",
            hover_name="product",
            title="Product Profitability: Revenue per Unit vs Quantity",
            labels={
                "quantity": "Total Quantity Sold",
                "revenue_per_unit": "Revenue per Unit ($)",
                "discount_percent": "Avg Discount %"
            }
        )
        
        fig.update_layout(height=400)
        return fig
    
    @render_widget
    def product_region_heatmap():
        data = get_sales_data()
        if data.empty:
            return None
        
        # Get top 15 products
        top_products = (
            data.groupby("product")["total_amount"]
            .sum()
            .nlargest(15)
            .index.tolist()
        )
        
        # Filter data and create pivot
        filtered_data = data[data["product"].isin(top_products)]
        heatmap_data = (
            filtered_data.groupby(["product", "region"])["total_amount"]
            .sum()
            .reset_index()
            .pivot(index="product", columns="region", values="total_amount")
            .fillna(0)
        )
        
        fig = px.imshow(
            heatmap_data.values,
            x=heatmap_data.columns,
            y=heatmap_data.index,
            color_continuous_scale="Blues",
            title="Product Sales by Region (Top 15 Products)",
            labels={"color": "Sales ($)"}
        )
        
        fig.update_layout(height=500)
        return fig
    
    # Data Tables
    @render.data_frame
    def regional_table():
        data = get_sales_data()
        if data.empty:
            return pd.DataFrame()
        
        regional_summary = (
            data.groupby("region")
            .agg({
                "total_amount": ["sum", "mean"],
                "quantity": "sum",
                "customer_id": ["count", "nunique"],
                "discount_percent": "mean"
            })
            .round(2)
        )
        
        # Flatten column names
        regional_summary.columns = [
            "total_sales", "avg_transaction", "total_quantity", 
            "total_transactions", "unique_customers", "avg_discount"
        ]
        
        return regional_summary.reset_index()
    
    @render.data_frame  
    def product_table():
        data = get_sales_data()
        if data.empty:
            return pd.DataFrame()
        
        product_summary = (
            data.groupby("product")
            .agg({
                "total_amount": "sum",
                "quantity": "sum", 
                "unit_price": "mean",
                "discount_percent": "mean",
                "customer_id": "count"
            })
            .round(2)
            .sort_values("total_amount", ascending=False)
            .head(20)
        )
        
        product_summary.columns = [
            "total_sales", "total_quantity", "avg_unit_price", 
            "avg_discount", "total_transactions"
        ]
        
        return product_summary.reset_index()

app = App(app_ui, server)