import pandas as pd
from datetime import datetime, timedelta
import random
import numpy as np
from faker import Faker

fake = Faker()

class SalesDataGenerator:
    def __init__(self, num_records: int = 10000, random_seed: int = 35487):
        self.num_records = num_records
        self.random_seed = random_seed
        
        # Set random seeds for reproducibility
        random.seed(random_seed)
        np.random.seed(random_seed)
        fake.seed_instance(random_seed)
        
        self.product_catalog = {
            'Laptop': {'price_range': (800, 2500), 'weight': 0.15, 'seasonal_factor': 1.2},
            'Desktop': {'price_range': (600, 1800), 'weight': 0.08, 'seasonal_factor': 1.0},
            'Monitor': {'price_range': (200, 800), 'weight': 0.12, 'seasonal_factor': 1.1},
            'Keyboard': {'price_range': (30, 200), 'weight': 0.18, 'seasonal_factor': 1.0},
            'Mouse': {'price_range': (20, 150), 'weight': 0.19, 'seasonal_factor': 1.0},
            'Headphones': {'price_range': (50, 400), 'weight': 0.15, 'seasonal_factor': 1.3},
            'Tablet': {'price_range': (300, 1200), 'weight': 0.10, 'seasonal_factor': 1.4},
            'Smartphone': {'price_range': (400, 1500), 'weight': 0.12, 'seasonal_factor': 1.5},
            'Printer': {'price_range': (150, 600), 'weight': 0.06, 'seasonal_factor': 0.9},
            'Webcam': {'price_range': (40, 250), 'weight': 0.08, 'seasonal_factor': 1.2},
            'Speaker': {'price_range': (80, 500), 'weight': 0.10, 'seasonal_factor': 1.1},
            'Router': {'price_range': (100, 400), 'weight': 0.05, 'seasonal_factor': 1.0}
        }
        self.regions = ['North', 'South', 'East', 'West', 'Central']
        self.sales_channels = ['Online', 'Retail', 'Partner', 'Direct']
        self.channel_weights = {'Online': 0.45, 'Retail': 0.30, 'Partner': 0.15, 'Direct': 0.10}
        
        # Customer segments with different behaviors
        self.customer_segments = {
            'Enterprise': {'weight': 0.15, 'avg_order_value': 2.5, 'bulk_probability': 0.8},
            'SMB': {'weight': 0.35, 'avg_order_value': 1.5, 'bulk_probability': 0.4},
            'Consumer': {'weight': 0.40, 'avg_order_value': 1.0, 'bulk_probability': 0.1},
            'Education': {'weight': 0.10, 'avg_order_value': 1.2, 'bulk_probability': 0.6}
        }
        
        # Regional economic factors
        self.regional_factors = {
            'North': {'economic_strength': 1.2, 'tech_adoption': 1.3},
            'South': {'economic_strength': 0.9, 'tech_adoption': 0.8},
            'East': {'economic_strength': 1.1, 'tech_adoption': 1.1},
            'West': {'economic_strength': 1.3, 'tech_adoption': 1.4},
            'Central': {'economic_strength': 1.0, 'tech_adoption': 1.0}
        }
        
        # Product bundles (products often bought together)
        self.product_bundles = {
            'Laptop': ['Mouse', 'Keyboard', 'Headphones'],
            'Desktop': ['Monitor', 'Keyboard', 'Mouse'],
            'Smartphone': ['Headphones', 'Speaker'],
            'Tablet': ['Keyboard', 'Headphones'],
            'Webcam': ['Headphones', 'Speaker']
        }
        
        # Top performing salespeople (create performance tiers)
        self.salesperson_tiers = {
            'top_performer': {'weight': 0.10, 'performance_mult': 1.8},
            'high_performer': {'weight': 0.20, 'performance_mult': 1.4},
            'average_performer': {'weight': 0.50, 'performance_mult': 1.0},
            'low_performer': {'weight': 0.20, 'performance_mult': 0.6}
        }
        
        # Generate fixed customer and salesperson pools
        self.max_customers = 500
        self.max_salespeople = 25
        self._generate_customer_pool()
        self._generate_salesperson_pool()
        
    def _generate_customer_pool(self):
        """Generate a fixed pool of customers with segments"""
        self.customers = []
        
        for _ in range(self.max_customers):
            # Select customer segment
            segment = self._get_customer_segment()
            
            customer = {
                'customer_id': fake.uuid4(),
                'customer_name': fake.name(),
                'customer_email': fake.email(),
                'customer_segment': segment
            }
            self.customers.append(customer)
    
    def _generate_salesperson_pool(self):
        """Generate a fixed pool of salespeople with performance tiers"""
        self.salespeople = []
        
        # Distribute salespeople across performance tiers
        tier_counts = {
            'top_performer': int(self.max_salespeople * 0.10),
            'high_performer': int(self.max_salespeople * 0.20),
            'low_performer': int(self.max_salespeople * 0.20),
            'average_performer': self.max_salespeople - int(self.max_salespeople * 0.50)
        }
        
        for tier, count in tier_counts.items():
            for _ in range(count):
                # Create salesperson name with performance hint
                base_name = fake.name()
                if tier == 'top_performer':
                    salesperson_name = base_name + ' (Top)'
                elif tier == 'high_performer':
                    salesperson_name = base_name + ' (High)'
                elif tier == 'low_performer':
                    salesperson_name = base_name + ' (Low)'
                else:
                    salesperson_name = base_name
                
                salesperson = {
                    'salesperson': salesperson_name,
                    'salesperson_tier': tier,
                    'assigned_customers': []
                }
                self.salespeople.append(salesperson)
        
        # Assign customers to salespeople (each salesperson gets ~20 customers)
        customers_per_salesperson = len(self.customers) // len(self.salespeople)
        
        for i, salesperson in enumerate(self.salespeople):
            start_idx = i * customers_per_salesperson
            end_idx = start_idx + customers_per_salesperson
            
            # Handle remainder for last salesperson
            if i == len(self.salespeople) - 1:
                end_idx = len(self.customers)
            
            salesperson['assigned_customers'] = self.customers[start_idx:end_idx]
    
    def _get_customer_and_salesperson(self):
        """Get a customer and their assigned salesperson"""
        # Select a random salesperson
        salesperson_data = random.choice(self.salespeople)
        
        # Select one of their assigned customers
        customer_data = random.choice(salesperson_data['assigned_customers'])
        
        return customer_data, salesperson_data
        
    def _get_seasonal_multiplier(self, date_obj, product):
        """Calculate seasonal multiplier based on date and product"""
        month = date_obj.month
        seasonal_factor = self.product_catalog[product]['seasonal_factor']
        
        # Holiday seasons (Nov-Dec) and back-to-school (Aug-Sep)
        if month in [11, 12]:
            return seasonal_factor * 1.5
        elif month in [8, 9]:
            return seasonal_factor * 1.2
        else:
            return seasonal_factor
    
    def _get_realistic_quantity(self, product):
        """Get realistic quantity based on product type"""
        if product in ['Laptop', 'Desktop', 'Tablet', 'Smartphone']:
            return np.random.choice([1, 2, 3], p=[0.8, 0.15, 0.05])
        elif product in ['Monitor', 'Printer']:
            return np.random.choice([1, 2, 3, 4], p=[0.7, 0.2, 0.08, 0.02])
        else:  # Accessories
            return np.random.choice([1, 2, 3, 4, 5, 6], p=[0.4, 0.3, 0.15, 0.08, 0.05, 0.02])
    
    def _get_realistic_discount(self, product, channel, quantity):
        """Get realistic discount based on product, channel, and quantity"""
        base_discount = 0
        
        # Channel-based discounts
        if channel == 'Partner':
            base_discount = np.random.uniform(5, 15)
        elif channel == 'Direct':
            base_discount = np.random.uniform(8, 20)
        elif channel == 'Online':
            base_discount = np.random.uniform(0, 10)
        else:  # Retail
            base_discount = np.random.uniform(0, 8)
        
        # Volume discount
        if quantity >= 5:
            base_discount += np.random.uniform(3, 8)
        elif quantity >= 3:
            base_discount += np.random.uniform(1, 5)
        
        # Product-specific discount probability
        if product in ['Laptop', 'Desktop', 'Smartphone']:
            # High-value items get discounts less frequently
            if random.random() < 0.3:
                return round(base_discount, 2)
            else:
                return 0
        else:
            # Accessories get discounts more frequently
            if random.random() < 0.6:
                return round(base_discount, 2)
            else:
                return 0
    
    def _get_customer_segment(self):
        """Select customer segment based on weights"""
        segments = list(self.customer_segments.keys())
        weights = [self.customer_segments[s]['weight'] for s in segments]
        return np.random.choice(segments, p=weights)
    
    def _get_salesperson_tier(self):
        """Select salesperson performance tier"""
        tiers = list(self.salesperson_tiers.keys())
        weights = [self.salesperson_tiers[t]['weight'] for t in tiers]
        return np.random.choice(tiers, p=weights)
    
    def _should_create_bundle(self, product, customer_segment):
        """Determine if this should be a bundle purchase"""
        if product not in self.product_bundles:
            return False
        
        # Enterprise and SMB customers more likely to buy bundles
        bundle_probability = {
            'Enterprise': 0.4,
            'SMB': 0.3,
            'Consumer': 0.15,
            'Education': 0.35
        }
        
        return random.random() < bundle_probability.get(customer_segment, 0.1)
    
    def _apply_regional_factors(self, price, region, product):
        """Apply regional economic and tech adoption factors"""
        regional_factor = self.regional_factors[region]
        
        # Tech products are more affected by tech adoption rates
        tech_products = ['Laptop', 'Desktop', 'Smartphone', 'Tablet', 'Webcam']
        
        if product in tech_products:
            multiplier = (regional_factor['economic_strength'] + regional_factor['tech_adoption']) / 2
        else:
            multiplier = regional_factor['economic_strength']
        
        return price * multiplier
    
    def generate_data(self) -> pd.DataFrame:
        """Generate synthetic sales data"""
        data = []
        
        # Create weighted product selection
        products = list(self.product_catalog.keys())
        weights = [self.product_catalog[p]['weight'] for p in products]
        
        # Normalize weights to sum to 1
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]
        
        # Create weighted channel selection
        channels = list(self.channel_weights.keys())
        channel_weights = list(self.channel_weights.values())
        
        bundle_transactions = []
        
        for _ in range(self.num_records):
            # Generate random date within the last 2 years
            start_date = datetime.now() - timedelta(days=730)
            random_date = start_date + timedelta(days=random.randint(0, 730))
            
            # Get customer and their assigned salesperson
            customer_data, salesperson_data = self._get_customer_and_salesperson()
            
            # Select product based on realistic market share
            product = np.random.choice(products, p=weights)
            
            # Select region
            region = random.choice(self.regions)
            
            # Get realistic price range for product
            price_range = self.product_catalog[product]['price_range']
            seasonal_mult = self._get_seasonal_multiplier(random_date, product)
            
            # Generate price with variation, seasonal adjustment, and regional factors
            base_price = np.random.uniform(price_range[0], price_range[1])
            adjusted_price = base_price * seasonal_mult
            adjusted_price = self._apply_regional_factors(adjusted_price, region, product)
            
            # Apply customer segment multiplier
            customer_segment = customer_data['customer_segment']
            segment_mult = self.customer_segments[customer_segment]['avg_order_value']
            unit_price = round(adjusted_price * segment_mult, 2)
            
            # Get realistic quantity (influenced by customer segment)
            base_quantity = self._get_realistic_quantity(product)
            if customer_segment == 'Enterprise' and random.random() < 0.3:
                quantity = base_quantity * random.randint(2, 5)  # Bulk orders
            elif customer_segment == 'Education' and random.random() < 0.4:
                quantity = base_quantity * random.randint(2, 3)  # Classroom sets
            else:
                quantity = base_quantity
            
            # Select sales channel based on realistic distribution
            sales_channel = np.random.choice(channels, p=channel_weights)
            
            # Get realistic discount
            discount_percent = self._get_realistic_discount(product, sales_channel, quantity)
            
            # Apply salesperson performance factor to discount
            salesperson_tier = salesperson_data['salesperson_tier']
            perf_mult = self.salesperson_tiers[salesperson_tier]['performance_mult']
            if perf_mult > 1.0:  # Good salespeople get better prices
                discount_percent *= 0.8
            elif perf_mult < 1.0:  # Poor salespeople give higher discounts
                discount_percent *= 1.3
            
            record = {
                'transaction_id': fake.uuid4(),
                'date': random_date.date(),
                'product': product,
                'quantity': quantity,
                'unit_price': unit_price,
                'customer_id': customer_data['customer_id'],
                'customer_name': customer_data['customer_name'],
                'customer_email': customer_data['customer_email'],
                'customer_segment': customer_segment,
                'region': region,
                'sales_channel': sales_channel,
                'salesperson': salesperson_data['salesperson'],
                'salesperson_tier': salesperson_tier,
                'discount_percent': round(discount_percent, 2),
            }
            
            # Calculate total amount
            subtotal = record['quantity'] * record['unit_price']
            discount_amount = subtotal * (record['discount_percent'] / 100)
            record['total_amount'] = round(subtotal - discount_amount, 2)
            
            data.append(record)
            
            # Check if this should trigger a bundle purchase
            if self._should_create_bundle(product, customer_segment):
                bundle_products = self.product_bundles[product]
                bundle_product = random.choice(bundle_products)
                
                # Create bundle transaction with same customer/date/salesperson
                bundle_record = record.copy()
                bundle_record['transaction_id'] = fake.uuid4()
                bundle_record['product'] = bundle_product
                bundle_record['quantity'] = self._get_realistic_quantity(bundle_product)
                
                # Bundle items often have different pricing
                bundle_price_range = self.product_catalog[bundle_product]['price_range']
                bundle_base_price = np.random.uniform(bundle_price_range[0], bundle_price_range[1])
                bundle_adjusted_price = bundle_base_price * seasonal_mult
                bundle_adjusted_price = self._apply_regional_factors(bundle_adjusted_price, region, bundle_product)
                bundle_record['unit_price'] = round(bundle_adjusted_price * segment_mult, 2)
                
                # Bundle discount (often better)
                bundle_discount = self._get_realistic_discount(bundle_product, sales_channel, bundle_record['quantity'])
                bundle_record['discount_percent'] = round(bundle_discount * 1.2, 2)  # Better bundle discount
                
                # Recalculate total
                bundle_subtotal = bundle_record['quantity'] * bundle_record['unit_price']
                bundle_discount_amount = bundle_subtotal * (bundle_record['discount_percent'] / 100)
                bundle_record['total_amount'] = round(bundle_subtotal - bundle_discount_amount, 2)
                
                bundle_transactions.append(bundle_record)
        
        # Add bundle transactions to main data
        data.extend(bundle_transactions)
        
        return pd.DataFrame(data)
    
    def save_to_parquet(self, df: pd.DataFrame, filename: str = 'sales.parquet'):
        """Save DataFrame to parquet format"""
        df.to_parquet(filename, engine='pyarrow', compression='snappy')
        print(f"Data saved to {filename}")
        return filename

def main():
    # Configuration
    NUM_RECORDS = 10000
    PARQUET_FILE = 'sales.parquet'
    
    # Generate synthetic data
    print("Generating synthetic sales data...")
    generator = SalesDataGenerator(NUM_RECORDS)
    df = generator.generate_data()
    
    print(f"Generated {len(df)} records")
    print(f"Data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Save to parquet
    print("\nSaving to parquet format...")
    generator.save_to_parquet(df, PARQUET_FILE)
    
    # Display sample data
    print("\nSample data:")
    print(df.head())
    
    print(f"\nParquet file saved as: {PARQUET_FILE}")

if __name__ == "__main__":
    main()