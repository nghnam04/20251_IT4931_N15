# twitter_scraper_fixed.py
import tweepy
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

class TwitterAPIScraper:
    def __init__(self):
        """Khởi tạo Twitter API client"""
        self.bearer_token = os.getenv('BEARER_TOKEN')
        
        if not self.bearer_token:
            print("⚠️ Warning: BEARER_TOKEN not found in .env file")
            print("Please create .env file with your Twitter API credentials")
            self.client = None
        else:
            # Khởi tạo client với Bearer Token
            self.client = tweepy.Client(
                bearer_token=self.bearer_token,
                wait_on_rate_limit=True
            )
            print("✅ Twitter API client initialized successfully")
    
    def search_tweets(self, query, max_results=100, start_time=None, end_time=None):
        """
        Tìm kiếm tweet với query
        """
        if self.client is None:
            print("❌ Twitter client not initialized. Check BEARER_TOKEN.")
            return None
        
        try:
            # Nếu không có start_time, mặc định 7 ngày trước
            if start_time is None:
                start_time = (datetime.now() - timedelta(days=7)).isoformat() + "Z"
            
            print(f"🔍 Searching tweets with query: {query}")
            print(f"📅 Time range: {start_time} to {end_time if end_time else 'now'}")
            
            # Gọi API
            tweets = self.client.search_recent_tweets(
                query=query,
                max_results=max_results,
                start_time=start_time,
                end_time=end_time,
                tweet_fields=[
                    'created_at', 'public_metrics', 'lang',
                    'source', 'context_annotations', 'entities',
                    'geo', 'attachments', 'referenced_tweets'
                ],
                expansions=[
                    'author_id', 
                    'attachments.media_keys',
                    'referenced_tweets.id.author_id'
                ],
                user_fields=[
                    'username', 'name', 'verified', 
                    'public_metrics', 'description', 'location'
                ],
                media_fields=['url', 'type', 'duration_ms', 'height', 'width']
            )
            
            if tweets.data:
                print(f"✅ Found {len(tweets.data)} tweets")
            else:
                print("⚠️ No tweets found")
            
            return tweets
            
        except tweepy.TweepyException as e:
            print(f"❌ Error searching tweets: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return None
    
    def process_tweets(self, tweets):
        """
        Xử lý và chuyển dữ liệu tweet thành DataFrame
        """
        if tweets is None or tweets.data is None:
            print("⚠️ No tweets to process")
            return pd.DataFrame()
        
        data = []
        
        # Lấy thông tin users
        users = {}
        if tweets.includes and 'users' in tweets.includes:
            users = {user.id: user for user in tweets.includes.get('users', [])}
        
        print(f"📊 Processing {len(tweets.data)} tweets...")
        
        for i, tweet in enumerate(tweets.data):
            user = users.get(tweet.author_id) if tweet.author_id else None
            
            # Trích xuất hashtags - SỬA LỖI
            hashtags = []
            if hasattr(tweet, 'entities') and tweet.entities:
                if 'hashtags' in tweet.entities and tweet.entities['hashtags']:
                    hashtags = [tag.get('tag', '') for tag in tweet.entities['hashtags'] if tag.get('tag')]
            
            # Trích xuất mentions - SỬA LỖI
            mentions = []
            if hasattr(tweet, 'entities') and tweet.entities:
                if 'mentions' in tweet.entities and tweet.entities['mentions']:
                    mentions = [mention.get('username', '') for mention in tweet.entities['mentions'] if mention.get('username')]
            
            # Trích xuất URLs - SỬA LỖI
            urls = []
            if hasattr(tweet, 'entities') and tweet.entities:
                if 'urls' in tweet.entities and tweet.entities['urls']:
                    urls = [url.get('expanded_url', '') for url in tweet.entities['urls'] if url.get('expanded_url')]
            
            # Lấy thông tin metrics an toàn - SỬA LỖI
            public_metrics = getattr(tweet, 'public_metrics', {})
            like_count = public_metrics.get('like_count', 0) if public_metrics else 0
            retweet_count = public_metrics.get('retweet_count', 0) if public_metrics else 0
            reply_count = public_metrics.get('reply_count', 0) if public_metrics else 0
            quote_count = public_metrics.get('quote_count', 0) if public_metrics else 0
            
            # Lấy thông tin user an toàn - SỬA LỖI
            user_metrics = getattr(user, 'public_metrics', {}) if user else {}
            user_followers = user_metrics.get('followers_count', 0) if user_metrics else 0
            user_following = user_metrics.get('following_count', 0) if user_metrics else 0
            user_tweet_count = user_metrics.get('tweet_count', 0) if user_metrics else 0
            
            # Tạo dictionary cho tweet
            tweet_data = {
                'tweet_id': getattr(tweet, 'id', ''),
                'text': getattr(tweet, 'text', ''),
                'created_at': getattr(tweet, 'created_at', None),
                'language': getattr(tweet, 'lang', ''),
                'source': getattr(tweet, 'source', ''),
                
                # Metrics - SỬA LỖI
                'like_count': like_count,
                'retweet_count': retweet_count,
                'reply_count': reply_count,
                'quote_count': quote_count,
                'impression_count': public_metrics.get('impression_count', 0) if public_metrics else 0,
                
                # User info - SỬA LỖI
                'user_id': getattr(tweet, 'author_id', ''),
                'username': getattr(user, 'username', '') if user else '',
                'user_name': getattr(user, 'name', '') if user else '',
                'user_description': getattr(user, 'description', '') if user else '',
                'user_location': getattr(user, 'location', '') if user else '',
                'user_verified': getattr(user, 'verified', False) if user else False,
                'user_followers': user_followers,
                'user_following': user_following,
                'user_tweet_count': user_tweet_count,
                
                # Entities - SỬA LỖI
                'hashtags': ', '.join(hashtags) if hashtags else '',
                'mentions': ', '.join(mentions) if mentions else '',
                'urls': ', '.join(urls) if urls else '',
                
                # Additional info - SỬA LỖI
                'is_retweet': self._is_retweet(tweet),
                'is_reply': self._is_reply(tweet),
                'has_media': self._has_media(tweet)
            }
            
            data.append(tweet_data)
            
            # Hiển thị progress
            if (i + 1) % 10 == 0:
                print(f"   Processed {i + 1}/{len(tweets.data)} tweets")
        
        df = pd.DataFrame(data)
        print(f"✅ Successfully processed {len(df)} tweets")
        return df
    
    def _is_retweet(self, tweet):
        """Kiểm tra nếu tweet là retweet - SỬA LỖI"""
        try:
            if hasattr(tweet, 'referenced_tweets') and tweet.referenced_tweets:
                for ref_tweet in tweet.referenced_tweets:
                    if hasattr(ref_tweet, 'type') and ref_tweet.type == 'retweeted':
                        return True
        except Exception as e:
            print(f"Debug _is_retweet error: {e}")
        return False
    
    def _is_reply(self, tweet):
        """Kiểm tra nếu tweet là reply - SỬA LỖI"""
        try:
            if hasattr(tweet, 'referenced_tweets') and tweet.referenced_tweets:
                for ref_tweet in tweet.referenced_tweets:
                    if hasattr(ref_tweet, 'type') and ref_tweet.type == 'replied_to':
                        return True
        except Exception as e:
            print(f"Debug _is_reply error: {e}")
        return False
    
    def _has_media(self, tweet):
        """Kiểm tra nếu tweet có media - SỬA LỖI"""
        try:
            if hasattr(tweet, 'attachments'):
                attachments = tweet.attachments
                if attachments and 'media_keys' in attachments:
                    return True
        except Exception as e:
            print(f"Debug _has_media error: {e}")
        return False
    
    def save_to_csv(self, df, filename=None):
        """
        Lưu DataFrame vào CSV file
        """
        if df.empty:
            print("⚠️ No data to save")
            return None
        
        # Tạo tên file nếu không có
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"twitter_data_{timestamp}.csv"
        
        try:
            # Clean text cho CSV
            if 'text' in df.columns:
                df['text'] = df['text'].astype(str).str.replace('\n', ' ').str.replace('\r', ' ')
            
            if 'user_description' in df.columns:
                df['user_description'] = df['user_description'].astype(str).str.replace('\n', ' ', regex=False)
            
            # Lưu vào CSV
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            print(f"💾 Data saved to: {filename}")
            print(f"📁 File size: {os.path.getsize(filename) / 1024:.2f} KB")
            
            return filename
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")
            return None
    
    def collect_by_keywords(self, keywords, max_tweets_per_keyword=100):
        """
        Thu thập tweet theo nhiều keywords - SỬA LỖI
        """
        if self.client is None:
            print("❌ Twitter client not initialized")
            return pd.DataFrame()
        
        all_data = pd.DataFrame()
        
        for keyword in keywords:
            print(f"\n{'='*50}")
            print(f"📌 Collecting tweets for keyword: {keyword}")
            print(f"{'='*50}")
            
            # Tìm kiếm tweets
            tweets = self.search_tweets(
                query=keyword,
                max_results=max_tweets_per_keyword
            )
            
            # Xử lý tweets
            if tweets:
                df = self.process_tweets(tweets)
            else:
                df = pd.DataFrame()
            
            if not df.empty:
                df['keyword'] = keyword
                all_data = pd.concat([all_data, df], ignore_index=True)
                
                # Lưu riêng cho từng keyword
                try:
                    keyword_clean = keyword.replace(' ', '_').replace('#', '').replace('OR', '_').replace('AND', '_')[:50]
                    keyword_filename = f"twitter_{keyword_clean}_{datetime.now().strftime('%Y%m%d')}.csv"
                    self.save_to_csv(df, keyword_filename)
                except Exception as e:
                    print(f"⚠️ Error saving keyword file: {e}")
            
            # Chờ để tránh rate limit
            time.sleep(2)
        
        # Lưu tất cả dữ liệu
        if not all_data.empty:
            try:
                summary_filename = f"twitter_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                self.save_to_csv(all_data, summary_filename)
                
                # Tạo báo cáo
                self.generate_report(all_data)
            except Exception as e:
                print(f"⚠️ Error saving summary: {e}")
        else:
            print("⚠️ No data collected from any keywords")
        
        return all_data
    
    def generate_report(self, df):
        """Tạo báo cáo thống kê"""
        try:
            report = {
                'total_tweets': len(df),
                'date_collected': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'unique_users': df['username'].nunique() if 'username' in df.columns else 0,
            }
            
            # Thêm các thống kê khác nếu có dữ liệu
            if 'created_at' in df.columns:
                report['date_range_tweets'] = f"{df['created_at'].min()} to {df['created_at'].max()}"
            
            if 'language' in df.columns:
                report['languages'] = df['language'].value_counts().to_dict()
            
            if 'like_count' in df.columns:
                report['avg_likes'] = float(df['like_count'].mean())
            
            if 'retweet_count' in df.columns:
                report['avg_retweets'] = float(df['retweet_count'].mean())
            
            if 'keyword' in df.columns:
                report['top_keywords'] = df['keyword'].value_counts().to_dict()
            
            if 'username' in df.columns:
                report['top_users'] = df['username'].value_counts().head(10).to_dict()
            
            if 'user_verified' in df.columns:
                report['verified_users'] = int(df['user_verified'].sum())
            
            if 'is_retweet' in df.columns:
                retweet_sum = df['is_retweet'].sum() if not df.empty else 0
                total_tweets = len(df)
                report['retweet_percentage'] = float((retweet_sum / total_tweets * 100) if total_tweets > 0 else 0)
            
            # Lưu báo cáo
            report_filename = f"twitter_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=4, ensure_ascii=False)
            
            print(f"\n📊 Report saved to: {report_filename}")
            
            # Hiển thị summary
            print("\n" + "="*50)
            print("📈 COLLECTION SUMMARY")
            print("="*50)
            print(f"Total tweets collected: {report['total_tweets']}")
            print(f"Unique users: {report['unique_users']}")
            
            if 'verified_users' in report:
                print(f"Verified users: {report['verified_users']}")
            
            if 'date_range_tweets' in report:
                print(f"Date range: {report['date_range_tweets']}")
            
            if 'avg_likes' in report:
                print(f"Average likes: {report['avg_likes']:.2f}")
            
            if 'avg_retweets' in report:
                print(f"Average retweets: {report['avg_retweets']:.2f}")
            
            if 'retweet_percentage' in report:
                print(f"Retweet percentage: {report['retweet_percentage']:.2f}%")
            
            print("="*50)
            
        except Exception as e:
            print(f"⚠️ Error generating report: {e}")

# ==================== HÀM CHÍNH ĐƠN GIẢN ====================
def main_simple():
    """Hàm chính đơn giản để test"""
    print("🚀 TWITTER DATA SCRAPER (SIMPLE VERSION)")
    print("="*50)
    
    # Kiểm tra .env file
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print("\nPlease create .env file with:")
        print("BEARER_TOKEN=your_bearer_token_here")
        
        # Tạo file .env mẫu
        with open('.env.sample', 'w') as f:
            f.write("BEARER_TOKEN=your_twitter_bearer_token_here\n")
            f.write("# Get token from: https://developer.twitter.com\n")
        
        print("\n✅ Created .env.sample file. Please rename to .env and add your token.")
        return
    
    # Khởi tạo scraper
    scraper = TwitterAPIScraper()
    
    if scraper.client is None:
        return
    
    # Test với query đơn giản
    test_queries = [
        "python",
        "#datascience",
        "công nghệ"
    ]
    
    print("\n📋 Testing with queries:")
    for query in test_queries:
        print(f"   • {query}")
    
    # Thu thập dữ liệu
    for query in test_queries:
        print(f"\n{'='*50}")
        print(f"Testing query: {query}")
        print(f"{'='*50}")
        
        tweets = scraper.search_tweets(query=query, max_results=10)
        
        if tweets and tweets.data:
            df = scraper.process_tweets(tweets)
            
            if not df.empty:
                print(f"\n✅ Collected {len(df)} tweets for '{query}'")
                
                # Hiển thị sample
                print("\nSample tweets:")
                for i, row in df.head(3).iterrows():
                    print(f"\n{i+1}. @{row['username']}")
                    print(f"   Text: {row['text'][:80]}...")
                    print(f"   Likes: {row['like_count']}, RTs: {row['retweet_count']}")
                
                # Lưu file
                filename = f"test_{query.replace(' ', '_').replace('#', '')}.csv"
                scraper.save_to_csv(df, filename)
            else:
                print(f"⚠️ No data processed for '{query}'")
        else:
            print(f"⚠️ No tweets found for '{query}'")

def main():
    """Hàm chính đầy đủ"""
    print("🚀 TWITTER DATA SCRAPER")
    print("="*50)
    
    # Khởi tạo scraper
    scraper = TwitterAPIScraper()
    
    if scraper.client is None:
        return
    
    # Danh sách keywords cần thu thập
    keywords = [
        "#python",
        "#programming",
        "machine learning",
        "công nghệ"
    ]
    
    # Thu thập dữ liệu
    print("\n📋 Collecting tweets for keywords:")
    for i, keyword in enumerate(keywords, 1):
        print(f"   {i}. {keyword}")
    
    max_tweets = 50  # Số tweet tối đa mỗi keyword (giảm để test)
    
    input("\nPress Enter to start collection...")
    
    # Bắt đầu thu thập
    all_data = scraper.collect_by_keywords(
        keywords=keywords,
        max_tweets_per_keyword=max_tweets
    )
    
    # Hiển thị kết quả
    if not all_data.empty:
        print("\n" + "="*50)
        print("🎉 COLLECTION COMPLETED SUCCESSFULLY!")
        print("="*50)
        
        # Hiển thị sample data
        print("\n📄 Sample of collected data:")
        print(all_data[['username', 'text', 'like_count', 'keyword']].head(3))
        
        print("\n📁 Files created:")
        for file in os.listdir('.'):
            if file.startswith('twitter_') and file.endswith('.csv'):
                print(f"   • {file}")
        
        print("\n✅ Ready for sentiment analysis!")
    else:
        print("\n⚠️ No data collected. Please check your query and try again.")

# ==================== CHẠY CHƯƠNG TRÌNH ====================
if __name__ == "__main__":
    # Chạy phiên bản đơn giản trước để test
    print("Choose mode:")
    print("1. Simple test mode (recommended)")
    print("2. Full collection mode")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        main_simple()
    else:
        main()