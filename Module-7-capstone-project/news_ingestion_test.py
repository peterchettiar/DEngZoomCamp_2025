"""
Python script to fetch news data related to a topic of choice and save it
locally as a .parquet file with sentiment analysis.
"""

# Standard library imports
import os
import argparse
import time
from datetime import datetime as dt
import warnings

# Third-party imports
import pandas as pd
from dotenv import load_dotenv
from newsapi import NewsApiClient
from tqdm import tqdm
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Load environment variables
load_dotenv()
warnings.filterwarnings("ignore")

def news_data_extraction(
    query: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    api_client: NewsApiClient
) -> pd.DataFrame:
    """Pulls news data from the API and returns it as a pandas DataFrame."""

    response = api_client.get_everything(
        q=query,
        from_param=start_date.strftime("%Y-%m-%d"),
        to=end_date.strftime("%Y-%m-%d"),
        sort_by="relevancy",
        page_size=30,
        language="en",
        page=1,
    )

    articles = response.get("articles", [])

    df = pd.DataFrame([
        {
            "Article_Source": article["source"]["name"],
            "Author": article["author"],
            "Title": article["title"],
            "Description": article["description"],
            "URL": article["url"],
            "Publish_Date": article["publishedAt"],
            "Summary": article["content"],
            "Job_Date": dt.today().date(),
        }
        for article in articles
    ])

    df["Publish_Date"] = pd.to_datetime(df["Publish_Date"]).dt.date
    df["Summary"] = df["Summary"].str.replace("\r\n", " ").str.extract(r"^(.*?)\s*\[")

    return df

def run_pipeline(query: str, days: int = 4) -> pd.DataFrame:
    """Main function to run the pipeline and return the final DataFrame."""

    print("Initializing API connection...")
    api_key = os.getenv("NEWSAPI_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Make sure NEWSAPI_API_KEY is set in your .env file.")

    api_client = NewsApiClient(api_key=api_key)
    start_time = time.time()

    date_ranges = pd.date_range(end=dt.today().date(), periods=days + 1)[::-1]
    all_articles = []

    for i in tqdm(range(len(date_ranges) - 1), desc="Fetching articles"):
        df = news_data_extraction(
            query=query,
            start_date=date_ranges[i + 1],
            end_date=date_ranges[i],
            api_client=api_client
        )
        all_articles.append(df)

    combined_df = pd.concat(all_articles, ignore_index=True)
    combined_df.sort_values(by="Publish_Date", ascending=False, inplace=True)
    combined_df.reset_index(drop=True, inplace=True)

    print("Performing sentiment analysis...")
    analyzer = SentimentIntensityAnalyzer()
    combined_df["Sentiment_Score"] = combined_df["Summary"].fillna("").apply(
        lambda text: analyzer.polarity_scores(text)["compound"]
    )
    combined_df["Sentiment"] = combined_df["Sentiment_Score"].apply(
        lambda score: "positive" if score > 0 else ("negative" if score < 0 else "neutral")
    )

    output_file = f"news_data_{dt.today().date()}.parquet"
    combined_df.to_parquet(output_file, index=False)

    duration = time.time() - start_time
    print(f"✅ News data saved to {output_file}. Completed in {duration:.2f} seconds.")

    return combined_df

def main():
    """Takes arguments and downloads related articles data into local directory"""
    parser = argparse.ArgumentParser(
        description="Fetch news articles based on a keyword and store in .parquet file."
    )
    parser.add_argument("--query", type=str, required=True, help="Search query for news articles")
    args = parser.parse_args()

    run_pipeline(query=args.query)

if __name__ == "__main__":
    main()
