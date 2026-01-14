import os
import time
import re
import json
from datetime import datetime
import pandas as pd
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
import google.generativeai as genai
import requests
from cerebras.cloud.sdk import Cerebras
from pymongo import MongoClient

# === API Key Configurations ===
GEMINI_API_KEY = "AIzaSyCzhycRBTdnDhS5mtvoo-InnpomlR7OhTc"
CEREBRAS_API_KEY = "csk-6k35r3try5ch842x4cy2e9vrnc54tj6jrhkkh3nrhpym4ntk"
OPENROUTER_API_KEY = "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"

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-1.5-flash")

# Configure Cerebras Client
cerebras_client = Cerebras(api_key=CEREBRAS_API_KEY)

# MongoDB Configuration
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["reviews_db"]

# === Original Utility Functions (EXACTLY THE SAME) ===

def is_clean_key(k):
    base_key = k.split("_")[0]
    return (
        len(base_key) > 3 and
        ".." not in base_key and
        not base_key.endswith("…") and
        not base_key.endswith(".") and
        not re.search(r"[.…]{2,}", base_key)
    )

def is_number(value):
    return bool(re.match(r"^\d+(\.\d+)?(\/\d+)?$", value.strip()))

def construct_Maps_url(restaurant_name, location):
    query = f"{restaurant_name} {location}".replace(" ", "+")
    return f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"

def wait_for_new_reviews(driver, prev_count, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        current_reviews = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
        if len(current_reviews) > prev_count:
            return len(current_reviews)
        time.sleep(1)
    return prev_count

def scrape_google_reviews(restaurant_name, location, existing_review_ids):
    options = webdriver.ChromeOptions()
    options.add_argument("--lang=en-US")
    # options.add_argument("--headless")
    options.add_argument("--disable-notifications")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    search_url = construct_Maps_url(restaurant_name, location)
    driver.get(search_url)

    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//button[contains(., "I agree") or contains(., "Accept all")]'))
        ).click()
        time.sleep(1)
    except TimeoutException:
        pass

    try:
        if "/place/" not in driver.current_url:
            first_result = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK.tH5CWc.THOPZb a"))
            )
            driver.get(first_result.get_attribute("href"))
            time.sleep(3)
    except TimeoutException:
        print("❌ Couldn't find the restaurant or navigate to its specific page.")
        driver.quit()
        return pd.DataFrame()

    try:
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(., "Reviews")]'))
        ).click()
        time.sleep(3)

        try:
            sort_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="Sort reviews"]'))
            )
            driver.execute_script("arguments[0].click();", sort_button)
            time.sleep(1)

            newest_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//div[@role="menuitemradio" and .//div[text()="Newest"]]'))
            )
            driver.execute_script("arguments[0].click();", newest_option)
            time.sleep(2)
        except TimeoutException:
            print("⚠️ Could not apply 'Newest' sorting — continuing with default sort.")
        
        # Scroll to load more reviews
        scroll_container = driver.find_element(By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf")
        for _ in range(5):  # Increased scroll count for list-style format
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_container)
            time.sleep(2)

    except TimeoutException:
        print("❌ Couldn't open reviews section for the restaurant.")
        driver.quit()
        return pd.DataFrame()

    def element_exists(parent, selector):
        try:
            parent.find_element(By.CSS_SELECTOR, selector)
            return True
        except:
            return False

    def get_total_reviews_count():
        try:
            container = driver.find_element(By.CSS_SELECTOR, "div.jANrlb")
            text = container.text
            match = re.search(r"([\d,]+)\s*reviews", text)
            if match:
                return int(match.group(1).replace(',', ''))
        except Exception as e:
            print(f"⚠️ Could not get total review count: {e}")
        return None

    total_reviews = get_total_reviews_count()
    if total_reviews:
        print(f"📊 Expected total reviews: {total_reviews:,}")

    reviews = []
    metadata_keys = set()
    found_duplicate_review_in_scrape = False

    print("🔄 Starting review collection...")

    while True:
        current_review_elements = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
        num_reviews_processed_in_this_pass = 0

        for review_element in current_review_elements[len(reviews):]:
            try:
                review_id = review_element.get_attribute("data-review-id")
                
                if review_id and review_id in existing_review_ids:
                    print(f"ℹ️ Encountered existing review ID '{review_id}'. Stopping scrape for new reviews.")
                    found_duplicate_review_in_scrape = True
                    break

                reviewer_info = review_element.find_element(By.CSS_SELECTOR, "div.RfnDt").text if element_exists(review_element, "div.RfnDt") else ""
                is_local_guide = "Y" if "Local Guide" in reviewer_info else "N"
                
                num_reviews = ""
                if "·" in reviewer_info:
                    parts = [p.strip() for p in reviewer_info.split("·")]
                    for part in parts:
                        if "review" in part.lower():
                            num_reviews = part.split()[0]
                            break

                if element_exists(review_element, "button.w8nwRe"):
                    driver.execute_script("arguments[0].click();", review_element.find_element(By.CSS_SELECTOR, "button.w8nwRe"))
                    time.sleep(0.2)

                review_text = review_element.find_element(By.CSS_SELECTOR, "span.wiI7pd").text if element_exists(review_element, "span.wiI7pd") else ""
                rating_elem = review_element.find_element(By.CSS_SELECTOR, "span.kvMYJc")
                rating_text = rating_elem.get_attribute("aria-label")
                rating = rating_text.split(" ")[0] if rating_text else ""

                owner_response = "None"
                if element_exists(review_element, "div.CDe7pd"):
                    owner_block = review_element.find_element(By.CSS_SELECTOR, "div.CDe7pd")
                    if element_exists(owner_block, "button.w8nwRe"):
                        driver.execute_script("arguments[0].click();", owner_block.find_element(By.CSS_SELECTOR, "button.w8nwRe"))
                        time.sleep(0.2)
                    owner_response = owner_block.text

                # ===== ENHANCED METADATA EXTRACTION =====
                metadata = {}
                
                # 1. Extract from structured metadata elements
                for item in review_element.find_elements(By.CSS_SELECTOR, "div.PBK6be"):
                    try:
                        key_elem = item.find_element(By.CSS_SELECTOR, "span.RfDO5c > span[style*='font-weight']")
                        value_elem = item.find_elements(By.CSS_SELECTOR, "span.RfDO5c")[-1]
                        
                        key = key_elem.text.strip(':').strip()
                        value = value_elem.text.strip()
                        if is_clean_key(key):
                            metadata[key] = value
                            metadata_keys.add(key)
                    except:
                        continue

                # 2. Extract from bold tags pattern (e.g., "<b>Service:</b> Excellent")
                for b_tag in review_element.find_elements(By.CSS_SELECTOR, "span > b"):
                    try:
                        full_text = b_tag.find_element(By.XPATH, "..").text
                        if ":" in full_text:
                            key, val = map(str.strip, full_text.split(":", 1))
                            if is_clean_key(key):
                                metadata[key] = val
                                metadata_keys.add(key)
                    except:
                        continue

                # 3. Extract from review text patterns (fallback)
                if not metadata and review_text:
                    patterns = {
                        "service": r"service\s*[:=]\s*([^\n,;]+)",
                        "wait_time": r"wait\s*time\s*[:=]\s*([^\n,;]+)", 
                        "food_quality": r"food\s*quality\s*[:=]\s*([^\n,;]+)"
                    }
                    
                    for key, pattern in patterns.items():
                        match = re.search(pattern, review_text, re.IGNORECASE)
                        if match and is_clean_key(key):
                            metadata[key] = match.group(1).strip()
                            metadata_keys.add(key)

                # Store the review data with metadata
                data = {
                    "review_id": review_id,
                    "reviewer_name": review_element.find_element(By.CSS_SELECTOR, "div.d4r55").text,
                    "review_date": review_element.find_element(By.CSS_SELECTOR, "span.rsqaWe").text,
                    "rating": rating,
                    "review_text": review_text,
                    "num_reviews": num_reviews,
                    "local_guide": is_local_guide,
                    "owner_response": owner_response,
                    "scrape_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "metadata": metadata if metadata else {"note": "no metadata found"}
                }
                
                
                reviews.append(data)
                num_reviews_processed_in_this_pass += 1
                
            except Exception as e:
                print(f"⚠️ Error processing review (ID: {review_id if 'review_id' in locals() else 'N/A'}): {str(e)}")
                continue
            
        if found_duplicate_review_in_scrape:
            break

        if num_reviews_processed_in_this_pass == 0:
            print(f"ℹ️ No new review elements found after scrolling or reached end of available reviews.")
            break

        try:
            scroll_container = driver.find_element(By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf")
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_container)
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Scroll error: {e}. Attempting alternative scroll for page.")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)

    print(f"📊 Final count: {len(reviews)} unique new/updated reviews collected from web.")
    
    # Ensure all metadata keys exist in all reviews
    metadata_keys = {k for k in metadata_keys if is_clean_key(k)}
    for review in reviews:
        for key in metadata_keys:
            if key not in review["metadata"]:
                review["metadata"][key] = "No data"

    driver.quit()

    # Convert to DataFrame
    reviews_df = pd.DataFrame(reviews)
    
    # Explode metadata into separate columns while preserving original structure
    if not reviews_df.empty and 'metadata' in reviews_df.columns:
        metadata_df = pd.json_normalize(reviews_df['metadata'])
        reviews_df = pd.concat([reviews_df.drop('metadata', axis=1), metadata_df], axis=1)
    
    return reviews_df

# === MongoDB Helper Functions ===

def get_or_create_restaurant_id(restaurant_name, location):
    prefix = (restaurant_name[:1] + location[:1]).upper()
    
    restaurant = db.restaurants.find_one({
        "restaurant_name": restaurant_name,
        "location": location
    })
    
    if restaurant:
        return restaurant["restaurant_id"]
    
    last_restaurant = db.restaurants.find_one(
        {"restaurant_id": {"$regex": f"^{prefix}"}},
        sort=[("restaurant_id", -1)]
    )
    
    if last_restaurant:
        last_id = last_restaurant["restaurant_id"]
        num = int(last_id[len(prefix):])
        new_num = num + 1
    else:
        new_num = 1
    
    new_restaurant_id = f"{prefix}{new_num:04d}"
    
    db.restaurants.insert_one({
        "restaurant_id": new_restaurant_id,
        "restaurant_name": restaurant_name,
        "location": location
    })
    
    return new_restaurant_id

def get_existing_review_ids(restaurant_id):
    existing_ids = set()
    if restaurant_id:
        try:
            reviews = db.reviews.find(
                {"restaurant_id": restaurant_id},
                {"review_id": 1}
            )
            for review in reviews:
                existing_ids.add(review["review_id"])
        except Exception as e:
            print(f"⚠️ Error fetching existing review IDs for {restaurant_id}: {e}")
    return existing_ids

def insert_reviews(restaurant_id, reviews_df):
    new_reviews_count = 0
    
    for _, row in reviews_df.iterrows():
        # Initialize base review document
        review_data = {
            "review_id": str(row['review_id']),
            "restaurant_id": str(restaurant_id),
            "reviewer_name": str(row.get('reviewer_name', '')),
            "review_date": row.get('review_date'),
            "rating": float(row['rating']) if pd.notna(row['rating']) else None,
            "review_text": str(row.get('review_text', '')),
            "num_reviews": str(row.get('num_reviews', '')),
            "local_guide": str(row.get('local_guide', 'N')),
            "owner_response": str(row.get('owner_response', '')),
            "scrape_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "metadata": {}  # Initialize metadata as empty dict
        }

        # Process metadata columns (all non-standard columns)
        standard_columns = {
            'review_id', 'reviewer_name', 'review_date', 'rating',
            'review_text', 'num_reviews', 'local_guide', 'owner_response',
            'scrape_timestamp'
        }

        for col in row.index:
            if col not in standard_columns and pd.notna(row[col]) and row[col] != 'No data':
                review_data["metadata"][col] = row[col]

        # Upsert the document
        result = db.reviews.update_one(
            {"review_id": review_data["review_id"], "restaurant_id": restaurant_id},
            {"$set": review_data},
            upsert=True
        )
        
        if result.upserted_id:
            new_reviews_count += 1
        elif result.modified_count:
            print(f"🔄 Updated existing review with new metadata")

    return new_reviews_count

def update_last_scraped_date(restaurant_id):
    today = datetime.now().strftime("%Y-%m-%d")
    db.restaurants.update_one(
        {"restaurant_id": restaurant_id},
        {"$set": {"last_scraped_at": today}}
    )

def update_summary_llm_column(restaurant_id, llm_column_name, summary_text):
    if llm_column_name not in ["GEMINI", "OPEN_ROUTER", "CEREBRAS"]:
        print(f"❌ ERROR: Invalid LLM column name provided: {llm_column_name}. Must be 'GEMINI', 'OPEN_ROUTER', or 'CEREBRAS'.")
        return

    print(f"DEBUG: Attempting to insert/update {llm_column_name} summary for {restaurant_id}...")
    
    db.summary.update_one(
        {"restaurant_id": restaurant_id},
        {"$set": {
            llm_column_name: summary_text,
            "updated_at": datetime.now()
        }},
        upsert=True
    )
    
    print(f"✅ {llm_column_name} summary for {restaurant_id} inserted/updated in 'summary' collection.")

def fetch_reviews_for_analysis(restaurant_id_to_analyze, local_guide_threshold=1000):
    try:
        # Count Local Guide reviews
        local_guide_count = db.reviews.count_documents({
            "restaurant_id": str(restaurant_id_to_analyze),
            "local_guide": "Y"
        })

        # Build query
        query = {"restaurant_id": str(restaurant_id_to_analyze)}
        if local_guide_count > local_guide_threshold:
            query["local_guide"] = "Y"
            reviews_type_used = "Local Guide Reviews"
        else:
            reviews_type_used = "All Reviews"

        # Get restaurant info
        restaurant = db.restaurants.find_one(
            {"restaurant_id": str(restaurant_id_to_analyze)},
            {"restaurant_name": 1, "location": 1}
        )

        # Fetch reviews with projection
        reviews = list(db.reviews.find(
            query,
            {
                "review_text": 1,
                "local_guide": 1,
                "owner_response": 1,
                "rating": 1,
                "metadata": 1,
                "_id": 0
            }
        ))

        if reviews:
            # Convert to DataFrame
            df = pd.DataFrame(reviews)
            
            # Ensure metadata exists as dict
            df['metadata'] = df['metadata'].apply(lambda x: x if isinstance(x, dict) else {})
            
            # Explode metadata into columns
            metadata_cols = pd.json_normalize(df['metadata'])
            df = pd.concat([df.drop('metadata', axis=1), metadata_cols], axis=1)
            
            # Ensure required columns exist
            for col in ['review_text', 'local_guide', 'owner_response', 'rating']:
                if col not in df.columns:
                    df[col] = None
            
            return df, restaurant["restaurant_name"], restaurant["location"], reviews_type_used

    except Exception as e:
        print(f"❌ Error fetching reviews: {str(e)}")
    
    return pd.DataFrame(), "Unknown Restaurant", "Unknown Location", "No Reviews"

# === MAIN EXECUTION LOGIC (EXACTLY THE SAME) ===

def main():
    input_file = "db_check.txt"
    pattern = re.compile(r'^\d+\.\s*(?P<restaurant_name>[^,]+),\s*(?P<location>.*)$')
    jobs = []

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            match = pattern.match(line)
            if match:
                original_rest_name = match.group('restaurant_name').strip()
                location = match.group('location').strip()
                
                is_veg_restaurant = False
                cleaned_rest_name = original_rest_name
                if re.search(r'\bveg\s+restaurant\b', original_rest_name, re.IGNORECASE):
                    is_veg_restaurant = True
                    cleaned_rest_name = re.sub(r'\s*\bveg\s+restaurant\b', '', original_rest_name, flags=re.IGNORECASE).strip()
                    if not cleaned_rest_name:
                        cleaned_rest_name = original_rest_name
                
                jobs.append((cleaned_rest_name, location, is_veg_restaurant, original_rest_name))
            else:
                print(f"⚠️ Skipping invalid line in {input_file}: {line}")

    print(f"\nTotal restaurants to process: {len(jobs)}")

    restaurants_to_consider_for_summary = []

    for cleaned_rest_name, location, is_veg_restaurant, original_rest_name in jobs:
        current_restaurant_id = None
        try:
            current_restaurant_id = get_or_create_restaurant_id(cleaned_rest_name, location)
            
            existing_review_ids = get_existing_review_ids(current_restaurant_id)
            
            print(f"\n🔍 Scraping '{original_rest_name}' in '{location}'...")
            reviews_df = scrape_google_reviews(cleaned_rest_name, location, existing_review_ids)
            
            if reviews_df.empty and existing_review_ids:
                print(f"ℹ️ No *new* reviews found for '{original_rest_name}'. Skipping DB insertion.")
                restaurants_to_consider_for_summary.append((current_restaurant_id, is_veg_restaurant, original_rest_name, 0))
                continue
            elif reviews_df.empty and not existing_review_ids:
                print(f"❌ No reviews found on Google Maps for '{original_rest_name}'. Skipping DB insertion/analysis trigger.")
                continue

            num_new_reviews_inserted = insert_reviews(current_restaurant_id, reviews_df)
            
            if num_new_reviews_inserted > 0:
                update_last_scraped_date(current_restaurant_id)
                print(f"✅ Inserted {num_new_reviews_inserted} NEW reviews into DB for restaurant ID: {current_restaurant_id}")
                restaurants_to_consider_for_summary.append((current_restaurant_id, is_veg_restaurant, original_rest_name, num_new_reviews_inserted))
            else:
                print(f"ℹ️ No genuinely new reviews were inserted for '{original_rest_name}' (only updates or no changes).")
                restaurants_to_consider_for_summary.append((current_restaurant_id, is_veg_restaurant, original_rest_name, 0))

        except Exception as e:
            print(f"❌ An error occurred processing '{original_rest_name}' in '{location}': {e}")

    print("\n🎉 All scraping and database insertion tasks completed.")
    
    if not restaurants_to_consider_for_summary:
        print("No restaurants to analyze or check for summary generation in this run.")
        return

    print("\n--- Starting AI Review Analysis ---")

    for res_id, is_veg_restaurant_flag, original_rest_name_for_context, num_new_reviews_inserted in restaurants_to_consider_for_summary:
        print(f"\n--- Analyzing reviews for '{original_rest_name_for_context}' (ID: {res_id}) ---")

        gemini_summary_text_output = ""
        cerebras_summary_text_output = ""
        openrouter_summary_text_output = ""

        existing_summary = db.summary.find_one(
            {"restaurant_id": res_id},
            {"GEMINI": 1, "OPEN_ROUTER": 1, "CEREBRAS": 1}
        )
        
        trigger_llm_analysis_block = False
        if num_new_reviews_inserted > 0:
            trigger_llm_analysis_block = True
        elif existing_summary is None:
            trigger_llm_analysis_block = True
        else:
            if existing_summary.get("GEMINI") is None or len(str(existing_summary.get("GEMINI", "")).strip()) == 0:
                trigger_llm_analysis_block = True
            if existing_summary.get("OPEN_ROUTER") is None or len(str(existing_summary.get("OPEN_ROUTER", "")).strip()) == 0:
                trigger_llm_analysis_block = True
            if existing_summary.get("CEREBRAS") is None or len(str(existing_summary.get("CEREBRAS", "")).strip()) == 0:
                trigger_llm_analysis_block = True
        
        if not trigger_llm_analysis_block:
            print(f"ℹ️ All LLM summaries exist for {res_id}, and no new reviews were added. Skipping AI analysis for this run.")
            continue

        df_analysis, res_name_analysis, res_loc_analysis, reviews_type_used = fetch_reviews_for_analysis(res_id)

        if 'review_text' in df_analysis.columns:
            df_analysis['review_text'] = df_analysis['review_text'].astype(str).str.strip()
        else:
            print("Warning: 'review_text' column not found in DataFrame")
            df_analysis['review_text'] = ""  # Add empty column if missing
        df_analysis = df_analysis[df_analysis['review_text'].str.len() > 0]
        df_analysis.drop_duplicates(subset='review_text', inplace=True)
        df_analysis.dropna(how='all', inplace=True)
        df_analysis.reset_index(drop=True, inplace=True)

        print(f"✅ Final {reviews_type_used} used for '{res_name_analysis}': {len(df_analysis)}")

        if df_analysis.empty:
            print(f"No suitable reviews found for '{res_name_analysis}' (ID: {res_id}) after filtering for analysis. Skipping AI analysis.")
            continue

        all_reviews = "\n".join(df_analysis['review_text'].tolist())
        owner_responses_exist = "Yes" if df_analysis['owner_response'].astype(str).str.contains(r'^(?!None$|^\s*$)').any() else "No"
        
        aggregated_metadata = {}
        for _, row in df_analysis.iterrows():
            # Safely handle metadata extraction
            meta = {}
            if 'metadata' in row:
                if isinstance(row['metadata'], str):
                    try:
                        meta = json.loads(row['metadata'])
                    except (json.JSONDecodeError, TypeError):
                        meta = {}
                elif isinstance(row['metadata'], dict):
                    meta = row['metadata']
            for key, value in meta.items():
                if key not in aggregated_metadata:
                    aggregated_metadata[key] = []
                aggregated_metadata[key].append(value)
        metadata_summary = ""
        if aggregated_metadata:
            metadata_summary += "\nAdditional Review Breakdown (if present in reviews):\n"
            for key, values in aggregated_metadata.items():
                unique_values = list(set(values))
                if len(unique_values) < 5:
                    metadata_summary += f"- {key}: {', '.join(map(str, unique_values))}\n"
                else:
                    metadata_summary += f"- {key}: (Diverse values mentioned across reviews)\n"

        veg_restaurant_intro = ""
        if is_veg_restaurant_flag:
            veg_restaurant_intro = "This restaurant is specifically identified as a **vegetarian restaurant**. Please pay close attention to feedback regarding vegetarian options and cuisine."

        prompt_for_llms = f"""
You are a professional data analyst and restaurant reviewer.

I will give you {reviews_type_used} for a restaurant. Based on the data, generate a structured review analysis report covering:

{veg_restaurant_intro}

1. Overview of the Restaurant
- **Name:** {res_name_analysis}
- **Location:** {res_loc_analysis}
- Cuisine type, service options (dine-in, buffet, etc.), price range
- Mention any special features or recurring highlights

2. Positives (✅)
Summarize major positive feedback based on reviews:
- Food quality (mention popular dishes)
- Service quality (staff behavior, owner responses)
- Value for money
- Vegetarian options (if mentioned)
{metadata_summary if "Food" in aggregated_metadata or "service" in aggregated_metadata else ""}

3. Negatives (❌)
Summarize major negative feedback based on reviews:
- Food quality (mention specific issues)
- Service quality (delays, rudeness, etc.)
- Ambience/Hygiene issues
- Pricing concerns

4. Actionable Insights for Restaurant Management (💡)
Based on the positives and negatives, provide specific, actionable recommendations for the restaurant to improve or maintain its quality. Focus on areas with clear trends in feedback.

5. Sentiment Analysis (⭐)
- Overall sentiment (positive, negative, mixed)
- Average rating (if numerical ratings are available, calculate and mention it).

6. Owner Engagement
Analyze the restaurant's owner response behavior based on the following data:

  1. Response Presence: {owner_responses_exist} (Yes/No)
  2. For responding owners:
     - Common response tones (positive/apologetic/defensive/neutral)
     - Specificity (personalized vs generic)
     - Improvement opportunities
  3. Key quote examples (if responses exist)

Base this ONLY on actual response data. If no responses exist, simply state "No owner responses found."

Instructions:
- Be concise but comprehensive. Use bullet points for readability.
- Maintain a professional and objective tone.
- If specific dishes are repeatedly mentioned positively or negatively, highlight them.
- If there's no data for a section, state "No specific feedback."
- The analysis should be solely based on the provided reviews. Do not use outside information.
- If you infer service options or price ranges, state that it's "inferred from reviews" if not explicitly mentioned.

Here are the reviews:
{all_reviews}
"""
        
        if existing_summary is None or existing_summary.get("GEMINI") is None or len(str(existing_summary.get("GEMINI", "")).strip()) == 0:
            print(f"✨ Generating Gemini summary for {res_id}...")
            try:
                gemini_response = gemini_model.generate_content(prompt_for_llms)
                gemini_summary_text_output = gemini_response.text
                update_summary_llm_column(res_id, "GEMINI", gemini_summary_text_output)
            except Exception as e:
                print(f"❌ Error generating Gemini summary for {res_id}: {e}")
        else:
            print(f"ℹ️ Gemini summary already exists for {res_id}. Skipping generation.")
            gemini_summary_text_output = existing_summary["GEMINI"]

        if existing_summary is None or existing_summary.get("OPEN_ROUTER") is None or len(str(existing_summary.get("OPEN_ROUTER", "")).strip()) == 0:
            print(f"✨ Generating OpenRouter summary for {res_id}...")
            try:
                openrouter_headers = {
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json"
                }
                openrouter_data = {
                    "model": "mistralai/mistral-7b-instruct:free",
                    "messages": [{"role": "user", "content": prompt_for_llms}]
                }
                openrouter_response = requests.post(
                    url="https://openrouter.ai/api/v1/chat/completions",
                    headers=openrouter_headers,
                    json=openrouter_data
                )
                openrouter_response.raise_for_status()
                openrouter_summary_text_output = openrouter_response.json()['choices'][0]['message']['content']
                update_summary_llm_column(res_id, "OPEN_ROUTER", openrouter_summary_text_output)
            except Exception as e:
                print(f"❌ Error generating OpenRouter summary for {res_id}: {e}")
        else:
            print(f"ℹ️ OpenRouter summary already exists for {res_id}. Skipping generation.")
            openrouter_summary_text_output = existing_summary["OPEN_ROUTER"]

        if existing_summary is None or existing_summary.get("CEREBRAS") is None or len(str(existing_summary.get("CEREBRAS", "")).strip()) == 0:
            print(f"✨ Generating Cerebras summary for {res_id}...")
            try:
                cerebras_response = cerebras_client.chat.completions.create(
                    model="llama-3.3-70b",
                    messages=[{"role": "user", "content": prompt_for_llms}]
                )
                cerebras_summary_text_output = cerebras_response.choices[0].message.content
                update_summary_llm_column(res_id, "CEREBRAS", cerebras_summary_text_output)
            except Exception as e:
                print(f"❌ Error generating Cerebras summary for {res_id}: {e}")
        else:
            print(f"ℹ️ Cerebras summary already exists for {res_id}. Skipping generation.")
            cerebras_summary_text_output = existing_summary["CEREBRAS"]

    print("\n--- All AI Review Analysis tasks completed. ---")

    summary_text = f"Summary for {name} in {location}"  # replace with actual generated summary
    return summary_text


if __name__ == "__main__":
    main()
