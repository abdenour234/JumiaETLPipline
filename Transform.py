import pandas as pd
import re
from textblob import TextBlob
from sklearn.cluster import KMeans
import joblib
import os

def transform(df, n_clusters=3, model_path='kmeans_model.pkl'):
    """
    Transforms the input DataFrame by cleaning, feature engineering, sentiment analysis,
    and applying K-means clustering.

    Parameters:
    df (pd.DataFrame): The input DataFrame to transform.
    n_clusters (int): Number of clusters for K-means (default is 3).
    model_path (str): Path to the saved K-means model.

    Returns:
    pd.DataFrame: Transformed DataFrame with additional columns.
    """

    # Transform 'Brand' column
    def transform_brand(x):
        if isinstance(x, str) and 'Marque:' in x:
            return x.replace("Marque:", "")
        else:
            return "Autre"
    df['Brand'] = df['Brand'].apply(transform_brand)

    # Convert 'Price' and 'Old Price' to numeric
    df['Price'] = pd.to_numeric(df['Price'].str.replace('Dhs', '', regex=False)
                                .str.replace(',', ''), errors='coerce')
    df['Old Price'] = pd.to_numeric(df['Old Price'].str.replace('Dhs', '', regex=False)
                                    .str.replace(',', ''), errors='coerce')

    # Clean 'Discount' column and calculate price difference
    df['Discount'] = df['Discount'].str.replace('%', '', regex=False)
    df['Rating']=df['Rating'].apply(lambda x:x.replace('out of 5',''))

    df['Price_Diff'] = df['Old Price'] - df['Price']

    # Categorize stock information
    def categorize_stock(x):
        if x in ['Nan', 'NaN']:
            return "No Info"
        elif x == "Disponible":
            return "High Stock"
        else:
            return "Low Stock"
    df['Stock'] = df['Stock'].apply(categorize_stock)

    # Extract numbers from 'Reviews'
    def extract_number(text):
        match = re.search(r'\d+', str(text))
        return int(match.group()) if match else 0
    df['Reviews'] = df['Reviews'].apply(extract_number)

    # Extract seller information
    def extract_seller_info(info):
        evaluation_match = re.search(r'(\d+)%', info)
        subscribers_match = re.search(r'(\d+) Abonnés', info)
        evaluation = int(evaluation_match.group(1)) if evaluation_match else 0
        subscribers = int(subscribers_match.group(1)) if subscribers_match else 0
        return pd.Series([evaluation, subscribers])
    df[['Seller_Evaluation', 'Seller_Abonne_Number']] = df['Seller Info'].apply(extract_seller_info)
    
    # Extract performance information
    def extract_performance_info(info):
        shipping_speed_match = re.search(r"Vitesse d'expédition:\s*(\w+\s\w+|\w+)", info)
        quality_score_match = re.search(r'Qualité:\s*(\w+\s\w+|\w+)', info)
        consumer_review_match = re.search(r'Avis des consommateurs:\s*(\w+\s\w+|\w+)', info)

        shipping_speed = shipping_speed_match.group(1).replace('Qualité', '') if shipping_speed_match else "neutre"
        quality_score = quality_score_match.group(1).split('Avis')[0] if quality_score_match else "neutre"
        consumer_review = consumer_review_match.group(1) if consumer_review_match else "neutre"

        return pd.Series([shipping_speed, quality_score, consumer_review])
    df[['Shipping_Speed', 'Quality_Score', 'Consumer_Review']] = df['Seller Performance'].apply(extract_performance_info)
    
    df=df.drop(columns=['Seller Performance', 'Seller Info'])
    # Sentiment analysis on comments
    def get_sentiment_score(text):
        blob = TextBlob(str(text))
        return blob.sentiment.polarity
    df['First Comment'] = df['First Comment'].apply(get_sentiment_score)
    df['Second Comment'] = df['Second Comment'].apply(get_sentiment_score)
    
    # Drop rows with missing values
    df = df[df['Price'].notna()]
    df = df.dropna()

    # Prepare for clustering: Drop non-numeric columns
    df_copy=df.drop(columns=['Name'])
    def label_encode(column):
        """Encodes a categorical column using Label Encoding."""
        unique_values = column.unique()
        encoding_dict = {value: index for index, value in enumerate(unique_values)}
        encoded_column = column.map(encoding_dict)
        return encoded_column
    
    df_copy['Brand']=label_encode(df_copy['Brand'])
    df_copy['Stock']=label_encode(df_copy['Stock'])
    df_copy['Shipping_Speed']=label_encode(df_copy['Shipping_Speed'])
    df_copy['Quality_Score']=label_encode(df_copy['Quality_Score'])
    df_copy['Consumer_Review']=label_encode(df_copy['Consumer_Review'])
    # Apply K-means clustering
    def apply_kmeans(df, n_clusters, model_path):
        if os.path.exists(model_path):
            kmeans = joblib.load(model_path)
        else:
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            kmeans.fit(df)
            joblib.dump(kmeans, model_path)
        
        df['Cluster'] = kmeans.predict(df)
        return df

    df_copy = apply_kmeans(df_copy, n_clusters, model_path)

    # Merge the 'Cluster' column back to the original DataFrame
    df['Cluster'] = df_copy['Cluster']

    return df

